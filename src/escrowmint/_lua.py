_COMMON = """\
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
"""

TRY_CONSUME = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]
local idem_key = KEYS[6]

local amount = tonumber(ARGV[1])
local operation_id = ARGV[2]
local idem_ttl_ms = tonumber(ARGV[3])
local request_fingerprint = ARGV[4]
local receipt_ttl_ms = tonumber(ARGV[5])

if amount == nil or amount <= 0 then
  return redis.error_reply("INVALID_AMOUNT")
end

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

if idem_key ~= nil and idem_key ~= "" then
  local existing = redis.call("GET", idem_key)
  if existing then
    local payload = decode_payload(existing)
    if not payload then
      return redis.error_reply("CORRUPT_STATE")
    end
    if payload["request_fingerprint"] ~= request_fingerprint then
      return redis.error_reply("DUPLICATE_IDEMPOTENCY_CONFLICT")
    end
    return cjson.encode({
      applied = payload["applied"],
      remaining = payload["remaining"],
      operation_id = payload["operation_id"],
    })
  end
end

local applied = false
local remaining = available

if available >= amount then
  remaining = available - amount
  applied = true
  version = version + 1
  persist_state(state_key, remaining, reserved, version)
end

local result = {
  applied = applied,
  remaining = remaining,
  operation_id = operation_id,
}

if idem_key ~= nil and idem_key ~= "" then
  local payload = {
    request_fingerprint = request_fingerprint,
    applied = applied,
    remaining = remaining,
    operation_id = operation_id,
  }
  redis.call("SET", idem_key, cjson.encode(payload), "PX", idem_ttl_ms)
end

return cjson.encode(result)
"""

TOP_UP = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]
local idem_key = KEYS[6]

local amount = tonumber(ARGV[1])
local operation_id = ARGV[2]
local idem_ttl_ms = tonumber(ARGV[3])
local request_fingerprint = ARGV[4]
local receipt_ttl_ms = tonumber(ARGV[5])

if amount == nil or amount <= 0 then
  return redis.error_reply("INVALID_AMOUNT")
end

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

if idem_key ~= nil and idem_key ~= "" then
  local existing = redis.call("GET", idem_key)
  if existing then
    local payload = decode_payload(existing)
    if not payload then
      return redis.error_reply("CORRUPT_STATE")
    end
    if payload["request_fingerprint"] ~= request_fingerprint then
      return redis.error_reply("DUPLICATE_IDEMPOTENCY_CONFLICT")
    end
    return cjson.encode({
      added = payload["added"],
      available = payload["available"],
      operation_id = payload["operation_id"],
    })
  end
end

available = available + amount
version = version + 1
persist_state(state_key, available, reserved, version)

local result = {
  added = amount,
  available = available,
  operation_id = operation_id,
}

if idem_key ~= nil and idem_key ~= "" then
  local payload = {
    request_fingerprint = request_fingerprint,
    added = amount,
    available = available,
    operation_id = operation_id,
  }
  redis.call("SET", idem_key, cjson.encode(payload), "PX", idem_ttl_ms)
end

return cjson.encode(result)
"""

RESERVE = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local amount = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local reservation_id = ARGV[3]
local receipt_ttl_ms = tonumber(ARGV[4])

if amount == nil or amount <= 0 then
  return redis.error_reply("INVALID_AMOUNT")
end

if ttl_ms == nil or ttl_ms <= 0 then
  return redis.error_reply("INVALID_TTL")
end

local available, reserved, version, current_ms, reclaim_err = reclaim_all(
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

local existing = redis.call("HGET", reservations_key, reservation_id)
if existing then
  local payload = decode_payload(existing)
  if not payload then
    return redis.error_reply("CORRUPT_STATE")
  end

  local existing_amount = tonumber(payload["amount"])
  local expires_at_ms = tonumber(payload["expires_at_ms"])
  if existing_amount == nil or expires_at_ms == nil then
    return redis.error_reply("CORRUPT_STATE")
  end

  if payload["status"] == "pending" and existing_amount == amount and expires_at_ms > current_ms then
    return cjson.encode(payload)
  end
  if payload["status"] == "pending" and expires_at_ms <= current_ms then
    return redis.error_reply("RESERVATION_EXPIRED")
  end
  return redis.error_reply("DUPLICATE_IDEMPOTENCY_CONFLICT")
end

local receipt, receipt_err = load_reservation_receipt(state_key, reservation_id)
if receipt_err then
  return redis.error_reply(receipt_err)
end
if receipt then
  if receipt["status"] == "expired" then
    return redis.error_reply("RESERVATION_EXPIRED")
  end
  if receipt["status"] == "committed" then
    return redis.error_reply("RESERVATION_ALREADY_COMMITTED")
  end
  return redis.error_reply("DUPLICATE_IDEMPOTENCY_CONFLICT")
end

if available < amount then
  return redis.error_reply("INSUFFICIENT_QUOTA")
end

local expires_at_ms = current_ms + ttl_ms
available = available - amount
reserved = reserved + amount
version = version + 1
persist_state(state_key, available, reserved, version)

local reservation = {
  reservation_id = reservation_id,
  resource = resource_from_state(state_key),
  amount = amount,
  expires_at_ms = expires_at_ms,
  status = "pending",
}

redis.call("HSET", reservations_key, reservation_id, cjson.encode(reservation))
redis.call("ZADD", reservation_expiries_key, expires_at_ms, reservation_id)

return cjson.encode(reservation)
"""

COMMIT = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local reservation_id = ARGV[1]
local operation_id = ARGV[2]
local receipt_ttl_ms = tonumber(ARGV[3])

local available, reserved, version, current_ms, reclaim_err = reclaim_all(
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

local existing = redis.call("HGET", reservations_key, reservation_id)
if not existing then
  local receipt, receipt_err = load_reservation_receipt(state_key, reservation_id)
  if receipt_err then
    return redis.error_reply(receipt_err)
  end
  if not receipt then
    return redis.error_reply("RESERVATION_NOT_FOUND")
  end
  if receipt["status"] == "expired" then
    return redis.error_reply("RESERVATION_EXPIRED")
  end
  if receipt["status"] == "committed" then
    return cjson.encode({
      applied = true,
      remaining = receipt["remaining"],
      operation_id = receipt["operation_id"],
    })
  end
  return redis.error_reply("RESERVATION_NOT_FOUND")
end

local payload = decode_payload(existing)
if not payload then
  return redis.error_reply("CORRUPT_STATE")
end

local expires_at_ms = tonumber(payload["expires_at_ms"])
local amount = tonumber(payload["amount"])
if expires_at_ms == nil or amount == nil then
  return redis.error_reply("CORRUPT_STATE")
end

if payload["status"] ~= "pending" then
  return redis.error_reply("CORRUPT_STATE")
end

if expires_at_ms <= current_ms then
  available = available + amount
  reserved = reserved - amount
  version = version + 1
  payload["status"] = "expired"
  persist_state(state_key, available, reserved, version)
  redis.call("HDEL", reservations_key, reservation_id)
  redis.call("ZREM", reservation_expiries_key, reservation_id)
  store_reservation_receipt(state_key, reservation_id, payload, receipt_ttl_ms)
  return redis.error_reply("RESERVATION_EXPIRED")
end

reserved = reserved - amount
version = version + 1
persist_state(state_key, available, reserved, version)
redis.call("HDEL", reservations_key, reservation_id)
redis.call("ZREM", reservation_expiries_key, reservation_id)
store_reservation_receipt(state_key, reservation_id, {
  reservation_id = reservation_id,
  resource = resource_from_state(state_key),
  amount = amount,
  expires_at_ms = expires_at_ms,
  status = "committed",
  operation_id = operation_id,
  remaining = available,
}, receipt_ttl_ms)

return cjson.encode({
  applied = true,
  remaining = available,
  operation_id = operation_id,
})
"""

CANCEL = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local reservation_id = ARGV[1]
local receipt_ttl_ms = tonumber(ARGV[2])

local available, reserved, version, current_ms, reclaim_err = reclaim_all(
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

local existing = redis.call("HGET", reservations_key, reservation_id)
if not existing then
  local receipt, receipt_err = load_reservation_receipt(state_key, reservation_id)
  if receipt_err then
    return redis.error_reply(receipt_err)
  end
  if not receipt then
    return cjson.encode({ canceled = false })
  end
  if receipt["status"] == "committed" then
    return cjson.encode({ canceled = false })
  end
  return cjson.encode({ canceled = true })
end

local payload = decode_payload(existing)
if not payload then
  return redis.error_reply("CORRUPT_STATE")
end

local amount = tonumber(payload["amount"])
local expires_at_ms = tonumber(payload["expires_at_ms"])
if amount == nil or expires_at_ms == nil then
  return redis.error_reply("CORRUPT_STATE")
end

if payload["status"] ~= "pending" then
  return redis.error_reply("CORRUPT_STATE")
end

if expires_at_ms <= current_ms then
  available = available + amount
  reserved = reserved - amount
  version = version + 1
  payload["status"] = "expired"
  persist_state(state_key, available, reserved, version)
  redis.call("HDEL", reservations_key, reservation_id)
  redis.call("ZREM", reservation_expiries_key, reservation_id)
  store_reservation_receipt(state_key, reservation_id, payload, receipt_ttl_ms)
  return cjson.encode({ canceled = true })
end

available = available + amount
reserved = reserved - amount
version = version + 1
persist_state(state_key, available, reserved, version)
redis.call("HDEL", reservations_key, reservation_id)
redis.call("ZREM", reservation_expiries_key, reservation_id)
store_reservation_receipt(state_key, reservation_id, {
  reservation_id = reservation_id,
  resource = resource_from_state(state_key),
  amount = amount,
  expires_at_ms = current_ms,
  status = "canceled",
}, receipt_ttl_ms)

return cjson.encode({ canceled = true })
"""

GET_STATE = _COMMON + """\
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
"""

ALLOCATE_CHUNK = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local amount = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local owner_id = ARGV[3]
local lease_id = ARGV[4]
local receipt_ttl_ms = tonumber(ARGV[5])

if amount == nil or amount <= 0 then
  return redis.error_reply("INVALID_AMOUNT")
end

if ttl_ms == nil or ttl_ms <= 0 then
  return redis.error_reply("INVALID_TTL")
end

if owner_id == nil or owner_id == "" then
  return redis.error_reply("INVALID_OWNER")
end

local available, reserved, version, current_ms, reclaim_err = reclaim_all(
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

local existing = redis.call("HGET", chunk_leases_key, lease_id)
if existing then
  local payload = decode_payload(existing)
  if not payload then
    return redis.error_reply("CORRUPT_STATE")
  end

  local granted = tonumber(payload["granted"])
  local expires_at_ms = tonumber(payload["expires_at_ms"])
  if granted == nil or expires_at_ms == nil then
    return redis.error_reply("CORRUPT_STATE")
  end

  if payload["status"] == "active"
    and payload["owner_id"] == owner_id
    and granted == amount
    and expires_at_ms > current_ms then
    return cjson.encode(payload)
  end
  if payload["status"] == "active" and expires_at_ms <= current_ms then
    return redis.error_reply("LEASE_EXPIRED")
  end
  return redis.error_reply("DUPLICATE_IDEMPOTENCY_CONFLICT")
end

local receipt, receipt_err = load_chunk_receipt(state_key, lease_id)
if receipt_err then
  return redis.error_reply(receipt_err)
end
if receipt then
  if receipt["status"] == "expired" then
    return redis.error_reply("LEASE_EXPIRED")
  end
  if receipt["status"] == "released" then
    return redis.error_reply("LEASE_ALREADY_RELEASED")
  end
  return redis.error_reply("DUPLICATE_IDEMPOTENCY_CONFLICT")
end

if available < amount then
  return redis.error_reply("INSUFFICIENT_QUOTA")
end

local expires_at_ms = current_ms + ttl_ms
available = available - amount
version = version + 1
persist_state(state_key, available, reserved, version)

local lease = {
  lease_id = lease_id,
  resource = resource_from_state(state_key),
  owner_id = owner_id,
  granted = amount,
  remaining = amount,
  expires_at_ms = expires_at_ms,
  status = "active",
}

redis.call("HSET", chunk_leases_key, lease_id, cjson.encode(lease))
redis.call("ZADD", chunk_expiries_key, expires_at_ms, lease_id)

return cjson.encode(lease)
"""

CONSUME_CHUNK = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local lease_id = ARGV[1]
local owner_id = ARGV[2]
local amount = tonumber(ARGV[3])
local receipt_ttl_ms = tonumber(ARGV[4])

if amount == nil or amount <= 0 then
  return redis.error_reply("INVALID_AMOUNT")
end

if owner_id == nil or owner_id == "" then
  return redis.error_reply("INVALID_OWNER")
end

local _, _, _, _, reclaim_err = reclaim_all(
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

local existing = redis.call("HGET", chunk_leases_key, lease_id)
if not existing then
  local receipt, receipt_err = load_chunk_receipt(state_key, lease_id)
  if receipt_err then
    return redis.error_reply(receipt_err)
  end
  if not receipt then
    return redis.error_reply("LEASE_NOT_FOUND")
  end
  if receipt["status"] == "expired" then
    return redis.error_reply("LEASE_EXPIRED")
  end
  if receipt["status"] == "released" then
    return redis.error_reply("LEASE_ALREADY_RELEASED")
  end
  return redis.error_reply("LEASE_NOT_FOUND")
end

local payload = decode_payload(existing)
if not payload then
  return redis.error_reply("CORRUPT_STATE")
end

local remaining = tonumber(payload["remaining"])
local expires_at_ms = tonumber(payload["expires_at_ms"])
if remaining == nil or expires_at_ms == nil then
  return redis.error_reply("CORRUPT_STATE")
end

if payload["status"] ~= "active" then
  return redis.error_reply("CORRUPT_STATE")
end
if payload["owner_id"] ~= owner_id then
  return redis.error_reply("OWNER_MISMATCH")
end
if expires_at_ms <= now_ms() then
  return redis.error_reply("LEASE_EXPIRED")
end
if remaining < amount then
  return cjson.encode({
    applied = false,
    lease_id = lease_id,
    remaining = remaining,
    expires_at_ms = expires_at_ms,
  })
end

payload["remaining"] = remaining - amount
redis.call("HSET", chunk_leases_key, lease_id, cjson.encode(payload))

return cjson.encode({
  applied = true,
  lease_id = lease_id,
  remaining = tonumber(payload["remaining"]),
  expires_at_ms = expires_at_ms,
})
"""

RENEW_CHUNK = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local lease_id = ARGV[1]
local owner_id = ARGV[2]
local ttl_ms = tonumber(ARGV[3])
local receipt_ttl_ms = tonumber(ARGV[4])

if ttl_ms == nil or ttl_ms <= 0 then
  return redis.error_reply("INVALID_TTL")
end

if owner_id == nil or owner_id == "" then
  return redis.error_reply("INVALID_OWNER")
end

local _, _, _, current_ms, reclaim_err = reclaim_all(
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

local existing = redis.call("HGET", chunk_leases_key, lease_id)
if not existing then
  local receipt, receipt_err = load_chunk_receipt(state_key, lease_id)
  if receipt_err then
    return redis.error_reply(receipt_err)
  end
  if not receipt then
    return redis.error_reply("LEASE_NOT_FOUND")
  end
  if receipt["status"] == "expired" then
    return redis.error_reply("LEASE_EXPIRED")
  end
  if receipt["status"] == "released" then
    return redis.error_reply("LEASE_ALREADY_RELEASED")
  end
  return redis.error_reply("LEASE_NOT_FOUND")
end

local payload = decode_payload(existing)
if not payload then
  return redis.error_reply("CORRUPT_STATE")
end

if payload["status"] ~= "active" then
  return redis.error_reply("CORRUPT_STATE")
end
if payload["owner_id"] ~= owner_id then
  return redis.error_reply("OWNER_MISMATCH")
end

local expires_at_ms = current_ms + ttl_ms
payload["expires_at_ms"] = expires_at_ms
redis.call("HSET", chunk_leases_key, lease_id, cjson.encode(payload))
redis.call("ZADD", chunk_expiries_key, expires_at_ms, lease_id)

return cjson.encode(payload)
"""

RELEASE_CHUNK = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local lease_id = ARGV[1]
local owner_id = ARGV[2]
local receipt_ttl_ms = tonumber(ARGV[3])

if owner_id == nil or owner_id == "" then
  return redis.error_reply("INVALID_OWNER")
end

local available, reserved, version, current_ms, reclaim_err = reclaim_all(
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

local existing = redis.call("HGET", chunk_leases_key, lease_id)
if not existing then
  local receipt, receipt_err = load_chunk_receipt(state_key, lease_id)
  if receipt_err then
    return redis.error_reply(receipt_err)
  end
  if not receipt then
    return redis.error_reply("LEASE_NOT_FOUND")
  end
  if receipt["owner_id"] ~= owner_id then
    return redis.error_reply("OWNER_MISMATCH")
  end
  return cjson.encode(receipt)
end

local payload = decode_payload(existing)
if not payload then
  return redis.error_reply("CORRUPT_STATE")
end

local remaining = tonumber(payload["remaining"])
local granted = tonumber(payload["granted"])
if remaining == nil or granted == nil then
  return redis.error_reply("CORRUPT_STATE")
end

if payload["status"] ~= "active" then
  return redis.error_reply("CORRUPT_STATE")
end
if payload["owner_id"] ~= owner_id then
  return redis.error_reply("OWNER_MISMATCH")
end

if remaining > 0 then
  available = available + remaining
  version = version + 1
  persist_state(state_key, available, reserved, version)
end

local receipt = {
  lease_id = lease_id,
  resource = resource_from_state(state_key),
  owner_id = owner_id,
  granted = granted,
  remaining = remaining,
  expires_at_ms = current_ms,
  status = "released",
}

redis.call("HDEL", chunk_leases_key, lease_id)
redis.call("ZREM", chunk_expiries_key, lease_id)
store_chunk_receipt(state_key, lease_id, receipt, receipt_ttl_ms)

return cjson.encode(receipt)
"""

GET_CHUNK = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local reservation_expiries_key = KEYS[3]
local chunk_leases_key = KEYS[4]
local chunk_expiries_key = KEYS[5]

local lease_id = ARGV[1]
local receipt_ttl_ms = tonumber(ARGV[2])

local _, _, _, _, reclaim_err = reclaim_all(
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

local existing = redis.call("HGET", chunk_leases_key, lease_id)
if existing then
  local payload = decode_payload(existing)
  if not payload then
    return redis.error_reply("CORRUPT_STATE")
  end
  return cjson.encode(payload)
end

local receipt, receipt_err = load_chunk_receipt(state_key, lease_id)
if receipt_err then
  return redis.error_reply(receipt_err)
end
if receipt then
  return cjson.encode(receipt)
end

return redis.error_reply("LEASE_NOT_FOUND")
"""
