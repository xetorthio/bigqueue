-- Vars

local tms = ARGV[1]
local topic = ARGV[2]
local consumerGroup = ARGV[3]
local visibilityWindow = ARGV[4]
if visibilityWindow == nil or tonumber(visibilityWindow) <= 0 then
    visibilityWindow=120
end

local topicKey="topics:"..topic
local topicHead="topics:"..topic..":head"
local consumerKey=topicKey..":consumers:"..consumerGroup
local lastPointer = consumerKey..":last"
local failsList = consumerKey..":fails"
local processingList = consumerKey..":processing"

-- Functions
function getExpireTime()
    return tms+visibilityWindow
end

function isEmpty(t)
    return table.getn(t) <=0
end

function getMessage(messageId)
    return redis.call("hgetall",topicKey..":messages:"..messageId)
end

function addToProcessing(messageId)
    local expire = getExpireTime()
    redis.call("zadd",consumerKey..":processing",expire,messageId)
end

function addIdToMessage(msgId,message)
    table.insert(message,"id")
    table.insert(message,msgId)
end

-- Main Code

---- Check for expired id's
local expired = redis.call("zrangebyscore",processingList,"-inf",tms)
for k,v in pairs(expired) do
    redis.call("lpush",failsList,v)
    redis.call("zrem",processingList,v)
end



-- Get take failed message without remove
local failed = redis.call("lrange",failsList,0,0) 
-- The message to be returned
local message 

if isEmpty(failed) then
    -- Standar flow if no failed found
   
    ---- Get last id
    local msgId = redis.call("get",lastPointer)
    message = getMessage(msgId)
    if isEmpty(message) then
        local topicHead = redis.call("get",topicKey..":head")
        if msgId <= topicHead then
            redis.call("incr",lastPointer)
            return {err="Message with id ["..msgId.."] was expired"}
        end
    else
        addToProcessing(msgId)
        redis.call("incr",lastPointer)
        addIdToMessage(msgId,message)
    end
else
    -- if a failed message found
    local msgId = failed[1]
    message = getMessage(msgId)
    if isEmpty(message) then
        return {err="Message with id ["..msgId.."] was expired"}
    end
    addToProcessing(msgId)
    addIdToMessage(msgId,message)
    -- Remove item from queue after it is set to processing
    redis.call("lpop",failsList)
end

return message


