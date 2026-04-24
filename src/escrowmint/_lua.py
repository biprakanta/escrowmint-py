_COMMON = """\
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

local function reclaim_expired(state_key, reservations_key)
  local available, reserved, version = load_state(state_key)
  local entries = redis.call("HGETALL", reservations_key)
  local current_ms = now_ms()
  local reclaimed = 0

  for i = 1, #entries, 2 do
    local reservation_id = entries[i]
    local payload = cjson.decode(entries[i + 1])
    if payload["status"] == "pending" and tonumber(payload["expires_at_ms"]) <= current_ms then
      local amount = tonumber(payload["amount"])
      available = available + amount
      reserved = reserved - amount
      payload["status"] = "expired"
      redis.call("HSET", reservations_key, reservation_id, cjson.encode(payload))
      reclaimed = reclaimed + 1
    end
  end

  if reclaimed > 0 then
    version = version + reclaimed
    persist_state(state_key, available, reserved, version)
  end

  return available, reserved, version, current_ms
end
"""

TRY_CONSUME = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]
local idem_key = KEYS[3]

local amount = tonumber(ARGV[1])
local operation_id = ARGV[2]
local idem_ttl_ms = tonumber(ARGV[3])
local request_fingerprint = ARGV[4]

if amount == nil or amount <= 0 then
  return redis.error_reply("INVALID_AMOUNT")
end

local available, reserved, version, current_ms = reclaim_expired(state_key, reservations_key)

if idem_key ~= nil and idem_key ~= "" then
  local existing = redis.call("GET", idem_key)
  if existing then
    local payload = cjson.decode(existing)
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

RESERVE = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]

local amount = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local reservation_id = ARGV[3]

if amount == nil or amount <= 0 then
  return redis.error_reply("INVALID_AMOUNT")
end

if ttl_ms == nil or ttl_ms <= 0 then
  return redis.error_reply("INVALID_TTL")
end

local available, reserved, version, current_ms = reclaim_expired(state_key, reservations_key)

local existing = redis.call("HGET", reservations_key, reservation_id)
if existing then
  local payload = cjson.decode(existing)
  local existing_amount = tonumber(payload["amount"])
  if payload["status"] == "pending" and existing_amount == amount and tonumber(payload["expires_at_ms"]) > current_ms then
    return cjson.encode(payload)
  end
  if payload["status"] == "expired" then
    return redis.error_reply("RESERVATION_EXPIRED")
  end
  if payload["status"] == "committed" then
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
  resource = string.match(state_key, "{(.*)}") or "",
  amount = amount,
  expires_at_ms = expires_at_ms,
  status = "pending",
}

redis.call("HSET", reservations_key, reservation_id, cjson.encode(reservation))

return cjson.encode(reservation)
"""

COMMIT = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]

local reservation_id = ARGV[1]
local operation_id = ARGV[2]

local available, reserved, version, current_ms = reclaim_expired(state_key, reservations_key)

local existing = redis.call("HGET", reservations_key, reservation_id)
if not existing then
  return redis.error_reply("RESERVATION_NOT_FOUND")
end

local payload = cjson.decode(existing)

if payload["status"] == "expired" then
  return redis.error_reply("RESERVATION_EXPIRED")
end

if payload["status"] == "committed" then
  return cjson.encode({
    applied = true,
    remaining = available,
    operation_id = payload["operation_id"],
  })
end

if payload["status"] ~= "pending" then
  return redis.error_reply("RESERVATION_NOT_FOUND")
end

if tonumber(payload["expires_at_ms"]) <= current_ms then
  local amount = tonumber(payload["amount"])
  available = available + amount
  reserved = reserved - amount
  version = version + 1
  payload["status"] = "expired"
  persist_state(state_key, available, reserved, version)
  redis.call("HSET", reservations_key, reservation_id, cjson.encode(payload))
  return redis.error_reply("RESERVATION_EXPIRED")
end

local amount = tonumber(payload["amount"])
reserved = reserved - amount
version = version + 1
payload["status"] = "committed"
payload["operation_id"] = operation_id
persist_state(state_key, available, reserved, version)
redis.call("HSET", reservations_key, reservation_id, cjson.encode(payload))

return cjson.encode({
  applied = true,
  remaining = available,
  operation_id = operation_id,
})
"""

CANCEL = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]

local reservation_id = ARGV[1]

local available, reserved, version, current_ms = reclaim_expired(state_key, reservations_key)

local existing = redis.call("HGET", reservations_key, reservation_id)
if not existing then
  return cjson.encode({ canceled = false })
end

local payload = cjson.decode(existing)

if payload["status"] == "canceled" or payload["status"] == "expired" then
  return cjson.encode({ canceled = true })
end

if payload["status"] == "committed" then
  return cjson.encode({ canceled = false })
end

local amount = tonumber(payload["amount"])
available = available + amount
reserved = reserved - amount
version = version + 1
payload["status"] = "canceled"
persist_state(state_key, available, reserved, version)
redis.call("HSET", reservations_key, reservation_id, cjson.encode(payload))

return cjson.encode({ canceled = true })
"""

GET_STATE = _COMMON + """\
local state_key = KEYS[1]
local reservations_key = KEYS[2]

local available, reserved, version, current_ms = reclaim_expired(state_key, reservations_key)

return cjson.encode({
  available = available,
  reserved = reserved,
  version = version,
})
"""
