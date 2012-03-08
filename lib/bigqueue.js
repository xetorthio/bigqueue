var redis = require('redis'),
    events = require('events'),
    fs = require('fs')

var TOPICS_KEY ="topics"

var POST_MESSAGE_LUA_SCRIPT

/**
 * The big queue client is the main object which we work
 * to use bigqueue
 */
function BigQueueClient(config,callback){
    this.redisConfig = config
    this.redisClients = []
    var self = this;
    self.connect()
}

BigQueueClient.prototype = new process.EventEmitter()

BigQueueClient.prototype.connect = function(){
    try{
        var redisConfig = this.redisConfig
        for(var configId in redisConfig){
            var config = redisConfig[configId]
            var client = redis.createClient(config.port, config.host, null)
            this.redisClients.push({"config":config, "client":client})
        }
        this.emit("connect")
    }catch(e){
        this.emit("error", e)
    }
}

BigQueueClient.prototype.createTopic = function(name, ttl, callback){
    var countExecs = 0;
    var self = this;
    try{
        this.getTopics(function(err,topics){
            self.emitIfError(err)
            if(contains(topics,name)){
                var err = new Error("Topic ["+name+"] already exist");
                callback(err)
                return;
            }
            self.withRedisClients(function(client,total){
                client.lpush(TOPICS_KEY,name,function(err){
                    self.emitIfError(err)
                    client.set(TOPICS_KEY+":"+name+":ttl",ttl, function(err){
                        self.emitIfError(err)
                        countExecs++;
                        if(countExecs >= total){
                            callback()
                        }
                    })
                })
            })
        })
    }catch(e){
        this.emit("error",e)
    }
}

BigQueueClient.prototype.getTopics = function(callback){
   var idx = Math.floor(Math.random()*this.redisClients.length)
   this.redisClients[idx].client.lrange(TOPICS_KEY,0,-1,function(err,result){
       callback(err,result);
   })
}

BigQueueClient.prototype.getConsumerGroups = function(topic,callback){
   var idx = Math.floor(Math.random()*this.redisClients.length)
   this.redisClients[idx].client.lrange(TOPICS_KEY+":"+topic+":consumers",0,-1,function(err,result){
       callback(err,result);
   })
}

BigQueueClient.prototype.createConsumerGroup = function(topic,group,callback){
    var countExecs = 0;
    var self = this;

    this.getTopics(function(err,result){
        self.emitIfError(err)
        if(!contains(result,topic)){
            callback(new Error("Topic ["+topic+"] doen't exist"))
            return
        }
        self.getConsumerGroups(topic,function(err,result){
            self.emitIfError(err)
            if(contains(result,group)){
               callback(new Error("Group ["+group+"] for topic ["+topic+"] already exist"))
               return
            }
            self.withRedisClients(function(client,total){
                client.lpush(TOPICS_KEY+":"+topic+":consumers",group,function(err){
                    if(err)
                        callback(err)
                    countExecs++;
                    if(countExecs>=total)
                        callback()
                })
            })
        })
    })
}

BigQueueClient.prototype.postMessage = function(topic,msg,callback){
    if(!msg.msg){
        callback(new Error("The message must contains the msg property"),null)
        return;
    }
    var clientIdx= getClientIdForMessage(msg, this.redisClients.length)
    msg.createTime = Date.now()
    var client = this.redisClients[clientIdx].client
    client.eval(POST_MESSAGE_LUA_SCRIPT,0,topic,JSON.stringify(msg),function(err,data){
        if(err){
            callback(err,undefined)
            return;
        }
        client.keys("*",function(err,data){
            console.log(data)
        })
        callback(undefined,data)
    })    

}

BigQueueClient.prototype.withRedisClients = function(callback){
    if(!this.redisClients){
        var err =new Error("No redis to create")
        this.emit("error",err)
        callback(err)
        return;
    }
    for(var c in this.redisClients){
        callback(this.redisClients[c].client, this.redisClients.length)
    }
}

BigQueueClient.prototype.emitIfError = function(err){
    if(err){
        this.emit("error",err)
        throw err;
    }
}

exports.BigQueueClient = BigQueueClient;


exports.createClient = function(config){
    fs.readFile("./lib/postMessage.lua",'ascii',function(err,fileStr){
     POST_MESSAGE_LUA_SCRIPT=fileStr
    })
    return new BigQueueClient(config)
}


contains = function(arr,obj){
    for(var i in arr){
        if(arr[i] === obj)
            return true
    }
    return false
}

getClientIdForMessage = function(msg,clients){
    var clientIdx
    if(msg.key){
        if(isNaN(msg.key)){
           clientIdx = msg.key.hashCode() % clients
        }else{
           clientIdx = msg.key % clients
        }                     
    }else{
        clientIdx = Math.floor(Math.random()*clients) 
    }
    return clientIdx
}

