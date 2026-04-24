-- KEYS[1] = state key
-- KEYS[2] = idempotency key, or empty string if not used
--
-- ARGV[1] = amount
-- ARGV[2] = operation_id
-- ARGV[3] = idempotency_ttl_ms
-- ARGV[4] = request_fingerprint
--
-- Returns a JSON object:
-- {
--   "applied": true|false,
--   "remaining": number,
--   "operation_id": string
-- }
--
-- Error replies:
--   INVALID_AMOUNT
--   DUPLICATE_IDEMPOTENCY_CONFLICT

local state_key = KEYS[1]
local idem_key = KEYS[2]

local amount = tonumber(ARGV[1])
local operation_id = ARGV[2]
local idem_ttl_ms = tonumber(ARGV[3])
local request_fingerprint = ARGV[4]

if amount == nil or amount <= 0 then
  return redis.error_reply("INVALID_AMOUNT")
end

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

local state = redis.call("HMGET", state_key, "available", "version")
local available = tonumber(state[1])
local version = tonumber(state[2])

if available == nil then
  available = 0
end

if version == nil then
  version = 0
end

local applied = false
local remaining = available

if available >= amount then
  remaining = available - amount
  applied = true
  redis.call("HSET", state_key, "available", remaining)
  redis.call("HINCRBY", state_key, "version", 1)
else
  remaining = available
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
