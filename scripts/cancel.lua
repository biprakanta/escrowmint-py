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
