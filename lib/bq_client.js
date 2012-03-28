var redis = require('redis'),
    fs = require('fs'),
    should = require('should'),
    events = require("events"),
    log = require("node-logging")
var TOTAL_SCRIPTS = 6
var redisScripts = {};
/**
 *   scripts load
**/
fs.readFile(__dirname+'/../scripts/getMessage.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["getMessage"] = strFile
})
fs.readFile(__dirname+'/../scripts/postMessage.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["postMessage"] = strFile
})
fs.readFile(__dirname+'/../scripts/createConsumer.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["createConsumerGroup"] = strFile
})
fs.readFile(__dirname+'/../scripts/createTopic.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["createTopic"] = strFile
})
fs.readFile(__dirname+'/../scripts/ack.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["ackMessage"] = strFile
})
fs.readFile(__dirname+'/../scripts/fail.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["failMessage"] = strFile
})

/**
 *   finish scripts load
**/



function BigQueueClient(rConf){
    events.EventEmitter.call( this );
    this.redisConf = rConf
    this.redisLoaded = false
    var self = this;
    self.emitReadyWhenAllLoaded()
}

BigQueueClient.prototype = new events.EventEmitter();

BigQueueClient.prototype.emitReadyWhenAllLoaded = function(){
    var self = this;
    var scriptsLoaded = (Object.keys(redisScripts).length == TOTAL_SCRIPTS)
    if(scriptsLoaded && this.redisLoaded){
        this.emit("ready")
    }else{
        process.nextTick(function(){
            self.emitReadyWhenAllLoaded()
        })
    }
}

BigQueueClient.prototype.connect = function(){
    var self = this;
    this.redisClient = redis.createClient(this.redisConf.port,this.redisConf.host)
    this.redisClient.retry_delay = 50 || this.redisConf.retryDelay
    this.redisClient.retry_backoff = 1
    this.redisClient.on("error",function(err){
        log.err("Error connecting to ["+log.pretty(this.redisConf)+"]")
        err["redisConf"] = self.redisConf
        process.nextTick(function(){
            self.emit("error",err)
        })
    })
    this.redisClient.on("connect",function(){
        self.emit("connect",self.redisConf)
    })
    this.redisClient.on("ready",function(){
        log.inf("Connected to redis ["+log.pretty(self.redisConf)+"]")
        self.redisLoaded = true;
    })
}

BigQueueClient.prototype.createTopic = function(topic, callback){
    var topic = arguments[0]
    var ttl = -1
    var callback
    if(arguments.length == 3){
        ttl = arguments[1]
        callback = arguments[2]
    }else{
        callback = arguments[1]
    }
        
    this.redisClient.eval(redisScripts["createTopic"],0,topic,ttl,function(err,data){
        if(err){
            log.err("Error creating topic ["+log.pretty(err)+"]")
            err={"err":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.createConsumerGroup = function(topic, consumerGroup,callback){
    this.redisClient.eval(redisScripts["createConsumerGroup"],0,topic,consumerGroup,function(err,data){
        if(err){
            log.err("Error creating group ["+consumerGroup+"] for topic ["+topic+"], error: "+log.pretty(err))
            err={"err":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.postMessage = function(topic, message,callback){
    if(message.msg == undefined){
        callCallback(callback,{err: "The message should have the 'msg' property"})
        return
    }
    this.redisClient.eval(redisScripts["postMessage"],0,topic,JSON.stringify(message),function(err,data){
        if(err){
            log.err("Error posting message ["+log.pretty(message)+"], error: "+log.pretty(err))
            callCallback(callback,err)
        }else{
            if(callback)
                callback(null,{"id":data})
        }
    })  
}

BigQueueClient.prototype.getMessage = function(topic,consumer,visibilityWindow,callback){
    visibilityWindow = visibilityWindow || -1
    var tms = Math.floor(new Date().getTime() / 1000)
    this.redisClient.eval(redisScripts["getMessage"],0,tms,topic,consumer,visibilityWindow,function(err,data){
        if(err){
            log.err("Error getting messages of topic ["+topic+"] for consumer ["+consumer+"], error: "+log.pretty(err))
            callCallback(callback,err)
        }else{
            if(callback){
                var data = redisListToObj(data)
                data.recipientCallback = data.id
                callback(null,data)
            }
        }
    })
}

BigQueueClient.prototype.ackMessage = function(topic,consumerGroup,id,callback){
    this.redisClient.eval(redisScripts["ackMessage"],0,topic,consumerGroup,id,function(err,data){
        if(err){
            log.err("Error doing ack for message ["+id+"] in topic ["+topic+"] for consumer ["+consumerGroup+"], error: "+log.pretty(err))
            callCallback(callback,err)
        }else{
            if(data<=0)
                callCallback(callback,{err:"no messaga was acked"})
            else
                callCallback(callback)
        }
        
    })
}

BigQueueClient.prototype.failMessage = function(topic,consumerGroup,id,callback){
    this.redisClient.eval(redisScripts["failMessage"],0,topic,consumerGroup,id,function(err,data){
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.listTopics = function(callback){
    this.redisClient.smembers("topics",function(err,data){
        if(err)
            log.err("Error listing topics ["+log.pretty(err)+"]")
        callCallback(callback,data)
    })
    
}

BigQueueClient.prototype.getConsumerGroups = function(topic,callback){
    var self = this
    this.redisClient.sismember("topics",topic,function(err,data){
        if(data == 0){
            callCallback(callback,{err:"Error topic not found"})
            return
        }
        self.redisClient.smembers("topics:"+topic+":consumers",function(err,data){
            if(err){
                log.err("Error getting consumer groups for topic ["+topic+"], error: "+log.pretty(err))
            }
            if(callback)
                callback(null,data)
        })
    })
}

BigQueueClient.prototype.shutdown = function(){
    var self = this
    this.redisClient.quit(function(err,data){
        if(!err){
            try{
                self.redisClient.end()
            }catch(e){
                log.err("Error shutting down")
            }
        }
    })
}

redisListToObj = function(list){
    var o = {}
    for(var i = 0; i< list.length; i=i+2){
        o[list[i]] = list[i+1]
    }
    return o;
}

callCallback = function(){
    if(arguments.length <=0 )
        return;
    var callback = arguments[0]
    var check = arguments[1]
    if(callback){
        if(check){
            callback(check)
        }else{
            callback()
        }
    }

}


exports.createClient = function(redisConf){
    var cli = new BigQueueClient(redisConf)
    cli.connect()
    return cli;
}

