local topics = redis.call('lrange','topics',0,-1) 
local topic = ARGV[1]
local jsonMessage = ARGV[2]

found = false
for key,t in pairs(topics) do
    if t == topic then 
        found = true  
    end 
end 
if not found then
    return {err='Topic ['..topic..'] not found'}  
end

topicKey = 'topics:'..topic
consumers = redis.call('lrange',topicKey..':consumers',0,-1) 
if table.getn(consumers) <1 then
    return -1
end

key = redis.call('incr',topicKey..':counterId') 
msgKey = 'messages:'..topic..':'..key 
ttl = redis.call('get',topicKey..':ttl') 
message = cjson.decode(jsonMessage)
for k,v in pairs(message) do
    redis.call('set',msgKey..':'..k,v) 
    redis.call('expire',msgKey..':'..k,ttl) 
end
for k,c in pairs(consumers) do 
    redis.call('rpush','consumers:'..topic..':'..c,key) 
end
return key
