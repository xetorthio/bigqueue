local topic = ARGV[1]
local jsonMessage = ARGV[2]
local topicKey = 'topics:'..topic
local key = redis.call('incr',topicKey..':head') 
local msgKey = topicKey..':messages:'..key 

-- If no ttl send an error
local ttl = redis.call('get',topicKey..':ttl')
if not ttl then
    return {err="Ttl for is not set for topic ["..topic.."]"}
end 

message = cjson.decode(jsonMessage)

if message["msg"] == nil then
    return {err="Message must contains the msg property"}
end

for k,v in pairs(message) do
    redis.call('hmset',msgKey,k,v) 
end
redis.call('expire',msgKey,ttl) 
return key
