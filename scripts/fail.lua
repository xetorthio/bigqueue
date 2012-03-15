local topic = ARGV[1]
local consumerGroup = ARGV[2]
local id = ARGV[3]

local topicKey = "topics:"..topic
local consumerKey = topicKey..":consumers:"..consumerGroup
local processingKey = consumerKey..":processing"
local failsKey = consumerKey..":fails"
local existTopic = redis.call("sismember","topics",topic)

if existTopic == 0 then
    return {err="Topic ["..topic.."] doesn't exist"}
end

local existProcessing = redis.call("exists",processingKey)
if existProcessing == 0 then
    return {err="Processing list for group ["..consumerGroup.."] of topic ["..topic.."] doesn't exist"}
end

redis.call("rpush",failsKey,id)
redis.call("zrem",processingKey,id)
