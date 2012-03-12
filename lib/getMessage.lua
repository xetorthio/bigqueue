-- Vars

local tms = ARGV[1]
local topic = ARGV[2]
local consumerGroup = ARGV[3]
local visibilityWindow = ARGV[4]
if visibilityWindow == nil then
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

function getMessage(topicKey,messageId)
    return redis.call("hgetall",topicKey..":messages:"..messageId)
end

function addToProcessing(messageId,expire)
    redis.call("zadd",consumerKey..":processing",tms,messageId)
end
-- Main Code

local lastId = redis.call("get",lastPointer)
if not lastId then
    return {err=consumerGroup.." for topic "..topic.." doesn't exist"}
end

local failed = redis.call("lrange",failsList,0,0) 

if isEmpty(failed) then
    -- Standar flow
    local message = getMessage(topicKey,lastId)
    if isEmpty(message) then
        local topicHead = redis.call("get",topicKey..":head")
        if lastId < topicHead then
            redis.call("incr",lastPointer)
        end
        return {}
    end
    local expire = getExpireTime()
    addToProcessing(lastId,expire)
    redis.call("incr",lastPointer)
    return message
else
    -- There are fails flow
    local messageId = getMessage(topicKey,failed[0])

    print("hay fallados")
end


