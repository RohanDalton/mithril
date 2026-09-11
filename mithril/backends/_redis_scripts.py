"""
Shared atomic Redis mutations with a separate deadline for each reader.
"""

__author__ = "Rohan B. Dalton"

_READERS = """
local function now_ms()
    local value = redis.call('TIME')
    return tonumber(value[1]) * 1000 + math.floor(tonumber(value[2]) / 1000)
end
local function prune(readers, deadlines, now)
    local expired = redis.call('ZRANGEBYSCORE', deadlines, '-inf', now)
    for _, owner in ipairs(expired) do
        redis.call('HDEL', readers, owner)
        redis.call('ZREM', deadlines, owner)
    end
end
local function refresh(readers, deadlines, now)
    local last = redis.call('ZREVRANGE', deadlines, 0, 0, 'WITHSCORES')
    if #last == 0 then
        redis.call('DEL', readers, deadlines)
        return false
    end
    local ttl = math.max(1, math.ceil(tonumber(last[2]) - now))
    redis.call('PEXPIRE', readers, ttl)
    redis.call('PEXPIRE', deadlines, ttl)
    return true
end
local function signal(key, ttl)
    redis.call('DEL', key)
    redis.call('LPUSH', key, 1)
    redis.call('PEXPIRE', key, ttl)
end
local function next_token(key)
    redis.call('INCR', key)
    redis.call('PERSIST', key)
    return redis.call('GET', key)
end
"""

ACQUIRE = (
    "local acquired = false\n"
    + _READERS
    + """
local now = now_ms()
prune(KEYS[5], KEYS[6], now)
if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('ZCARD', KEYS[6]) > 0 then
    return {-1, -1}
end
local token = next_token(KEYS[2])
redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
redis.call('DEL', KEYS[4])
redis.call('HSET', KEYS[4], 'owner', ARGV[1], 'epoch', token,
           'token', token, 'acquired_at', ARGV[4])
redis.call('PEXPIRE', KEYS[4], ARGV[2])
return {token, token}
"""
)

ACQUIRE_READ = (
    _READERS
    + """
local now = now_ms()
prune(KEYS[5], KEYS[6], now)
if redis.call('EXISTS', KEYS[1]) == 1 then return {-1, -1} end
local token = next_token(KEYS[2])
redis.call('HINCRBY', KEYS[5], ARGV[1], 1)
redis.call('ZADD', KEYS[6], now + tonumber(ARGV[2]), ARGV[1])
refresh(KEYS[5], KEYS[6], now)
return {token, token}
"""
)

RELEASE = (
    _READERS
    + """
local current = redis.call('GET', KEYS[1])
if not current then return 2 end
if current ~= ARGV[1] then return 1 end
redis.call('DEL', KEYS[1], KEYS[3])
signal(KEYS[2], ARGV[2])
return 0
"""
)

RELEASE_READ = (
    _READERS
    + """
local now = now_ms()
prune(KEYS[4], KEYS[5], now)
local count = redis.call('HGET', KEYS[4], ARGV[1])
if not count then return 1 end
if tonumber(count) > 1 then
    redis.call('HINCRBY', KEYS[4], ARGV[1], -1)
else
    redis.call('HDEL', KEYS[4], ARGV[1])
    redis.call('ZREM', KEYS[5], ARGV[1])
end
if not refresh(KEYS[4], KEYS[5], now) then signal(KEYS[2], ARGV[2]) end
return 0
"""
)

RENEW = (
    _READERS
    + """
local current = redis.call('GET', KEYS[1])
if not current then return 2 end
if current ~= ARGV[1] then return 1 end
if redis.call('PTTL', KEYS[1]) < 0 then return 2 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
redis.call('PERSIST', KEYS[5])
return 0
"""
)

RENEW_READ = (
    _READERS
    + """
local now = now_ms()
prune(KEYS[3], KEYS[4], now)
if not redis.call('ZSCORE', KEYS[4], ARGV[1]) then return 1 end
redis.call('ZADD', KEYS[4], now + tonumber(ARGV[2]), ARGV[1])
refresh(KEYS[3], KEYS[4], now)
redis.call('PERSIST', KEYS[5])
return 0
"""
)

IS_LOCKED = (
    _READERS
    + """
prune(KEYS[2], KEYS[3], now_ms())
if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('ZCARD', KEYS[3]) > 0 then
    return 1
end
return 0
"""
)
