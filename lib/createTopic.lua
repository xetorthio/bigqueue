local topic = ARGV[1]
local ttl = ARGV[2]
if ttl == nil then
    -- Default ttl one day
    ttl = 24*60*60
end

local exist = redis.call("sismember","topics",topic)

if exist == 1 then
    return {err="Topic ["..topic.."] already exist"}
end

redis.call("set","topics:"..topic..":ttl",ttl)
redis.call("sadd","topics",topic)
return
