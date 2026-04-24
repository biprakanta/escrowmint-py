local MAX_RECLAIM_BATCH = 128

local function now_ms()
  local t = redis.call("TIME")
  return (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)
end

local function load_state(state_key)
  local state = redis.call("HMGET", state_key, "available", "reserved", "version")
  local available = tonumber(state[1]) or 0
  local reserved = tonumber(state[2]) or 0
  local version = tonumber(state[3]) or 0
  return available, reserved, version
end

local function persist_state(state_key, available, reserved, version)
  if reserved < 0 then
    reserved = 0
  end

  redis.call(
    "HSET",
    state_key,
    "available", available,
    "reserved", reserved,
    "version", version
  )
end

local function decode_payload(raw)
  local ok, payload = pcall(cjson.decode, raw)
  if not ok then
    return nil
  end
  return payload
end

local function resource_from_state(state_key)
  return string.match(state_key, "{(.*)}") or ""
end

local function reservation_receipt_key_for(state_key, reservation_id)
  local receipt_key = string.gsub(state_key, ":state$", ":receipt:" .. reservation_id)
  return receipt_key
end

local function chunk_receipt_key_for(state_key, lease_id)
  local receipt_key = string.gsub(state_key, ":state$", ":chunk_receipt:" .. lease_id)
  return receipt_key
end

local function load_reservation_receipt(state_key, reservation_id)
  local raw = redis.call("GET", reservation_receipt_key_for(state_key, reservation_id))
  if not raw then
    return nil, nil
  end

  local payload = decode_payload(raw)
  if not payload then
    return nil, "CORRUPT_STATE"
  end

  return payload, nil
end

local function load_chunk_receipt(state_key, lease_id)
  local raw = redis.call("GET", chunk_receipt_key_for(state_key, lease_id))
  if not raw then
    return nil, nil
  end

  local payload = decode_payload(raw)
  if not payload then
    return nil, "CORRUPT_STATE"
  end

  return payload, nil
end

local function store_reservation_receipt(state_key, reservation_id, payload, receipt_ttl_ms)
  redis.call(
    "SET",
    reservation_receipt_key_for(state_key, reservation_id),
    cjson.encode(payload),
    "PX",
    receipt_ttl_ms
  )
end

local function store_chunk_receipt(state_key, lease_id, payload, receipt_ttl_ms)
  redis.call(
    "SET",
    chunk_receipt_key_for(state_key, lease_id),
    cjson.encode(payload),
    "PX",
    receipt_ttl_ms
  )
end

local function reclaim_expired_reservations(
  state_key,
  reservations_key,
  reservation_expiries_key,
  receipt_ttl_ms,
  available,
  reserved,
  version,
  current_ms
)
  local reclaimed = 0
  local expired_ids = redis.call(
    "ZRANGEBYSCORE",
    reservation_expiries_key,
    "-inf",
    current_ms,
    "LIMIT",
    0,
    MAX_RECLAIM_BATCH
  )

  for _, reservation_id in ipairs(expired_ids) do
    local raw = redis.call("HGET", reservations_key, reservation_id)
    if not raw then
      redis.call("ZREM", reservation_expiries_key, reservation_id)
    else
      local payload = decode_payload(raw)
      if not payload then
        return nil, nil, nil, "CORRUPT_STATE"
      end

      local expires_at_ms = tonumber(payload["expires_at_ms"])
      if expires_at_ms == nil then
        return nil, nil, nil, "CORRUPT_STATE"
      end

      if payload["status"] == "pending" and expires_at_ms <= current_ms then
        local amount = tonumber(payload["amount"])
        if amount == nil then
          return nil, nil, nil, "CORRUPT_STATE"
        end

        available = available + amount
        reserved = reserved - amount
        payload["status"] = "expired"
        redis.call("HDEL", reservations_key, reservation_id)
        redis.call("ZREM", reservation_expiries_key, reservation_id)
        store_reservation_receipt(state_key, reservation_id, payload, receipt_ttl_ms)
        reclaimed = reclaimed + 1
      elseif payload["status"] ~= "pending" then
        redis.call("HDEL", reservations_key, reservation_id)
        redis.call("ZREM", reservation_expiries_key, reservation_id)
      else
        redis.call("ZADD", reservation_expiries_key, expires_at_ms, reservation_id)
      end
    end
  end

  if reclaimed > 0 then
    version = version + reclaimed
    persist_state(state_key, available, reserved, version)
  end

  return available, reserved, version, nil
end

local function reclaim_expired_chunks(
  state_key,
  chunk_leases_key,
  chunk_expiries_key,
  receipt_ttl_ms,
  available,
  reserved,
  version,
  current_ms
)
  local reclaimed = 0
  local expired_ids = redis.call(
    "ZRANGEBYSCORE",
    chunk_expiries_key,
    "-inf",
    current_ms,
    "LIMIT",
    0,
    MAX_RECLAIM_BATCH
  )

  for _, lease_id in ipairs(expired_ids) do
    local raw = redis.call("HGET", chunk_leases_key, lease_id)
    if not raw then
      redis.call("ZREM", chunk_expiries_key, lease_id)
    else
      local payload = decode_payload(raw)
      if not payload then
        return nil, nil, nil, "CORRUPT_STATE"
      end

      local expires_at_ms = tonumber(payload["expires_at_ms"])
      local remaining = tonumber(payload["remaining"])
      local granted = tonumber(payload["granted"])
      if expires_at_ms == nil or remaining == nil or granted == nil then
        return nil, nil, nil, "CORRUPT_STATE"
      end

      if payload["status"] == "active" and expires_at_ms <= current_ms then
        if remaining > 0 then
          available = available + remaining
          reclaimed = reclaimed + 1
        end
        payload["status"] = "expired"
        redis.call("HDEL", chunk_leases_key, lease_id)
        redis.call("ZREM", chunk_expiries_key, lease_id)
        store_chunk_receipt(state_key, lease_id, payload, receipt_ttl_ms)
      elseif payload["status"] ~= "active" then
        redis.call("HDEL", chunk_leases_key, lease_id)
        redis.call("ZREM", chunk_expiries_key, lease_id)
      else
        redis.call("ZADD", chunk_expiries_key, expires_at_ms, lease_id)
      end
    end
  end

  if reclaimed > 0 then
    version = version + reclaimed
    persist_state(state_key, available, reserved, version)
  end

  return available, reserved, version, nil
end

local function reclaim_all(
  state_key,
  reservations_key,
  reservation_expiries_key,
  chunk_leases_key,
  chunk_expiries_key,
  receipt_ttl_ms
)
  local available, reserved, version = load_state(state_key)
  local current_ms = now_ms()
  local reclaim_err = nil

  available, reserved, version, reclaim_err = reclaim_expired_reservations(
    state_key,
    reservations_key,
    reservation_expiries_key,
    receipt_ttl_ms,
    available,
    reserved,
    version,
    current_ms
  )
  if reclaim_err then
    return nil, nil, nil, current_ms, reclaim_err
  end

  available, reserved, version, reclaim_err = reclaim_expired_chunks(
    state_key,
    chunk_leases_key,
    chunk_expiries_key,
    receipt_ttl_ms,
    available,
    reserved,
    version,
    current_ms
  )
  if reclaim_err then
    return nil, nil, nil, current_ms, reclaim_err
  end

  return available, reserved, version, current_ms, nil
end
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local receipt_ttl_ms = tonumber(ARGV[1])

local available, reserved, version, _, reclaim_err = reclaim_all(
  state_key,
  reservations_key,
  reservation_expiries_key,
  chunk_leases_key,
  chunk_expiries_key,
  receipt_ttl_ms
)
if reclaim_err then
  return redis.error_reply(reclaim_err)
end

return cjson.encode({
  available = available,
  reserved = reserved,
  version = version,
})

