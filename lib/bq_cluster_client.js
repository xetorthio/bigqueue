var redis = require('redis'),
    fs = require('fs'),
    should = require('should'),
    events = require("events"),
    ZK = require ("zookeeper"),
    ZKMonitor = require("../lib/zk_monitor.js"),
    os = require("os"),
    log = require("node-logging")

var topicsPath = "/topics"
var consumerGroupsPath = "/consumerGroups"

/**
 *  Object responsible for execute all commands throght the bq cluster
 */
function BigQueueClusterClient(zkClient,zkClusterPath,zkFlags,createClientFunction,sentCacheTime){

    this.nodes=[]

    this.zkClient = zkClient
    this.zkClusterPath = zkClusterPath
    this.zkFlags = 0 | zkFlags
    this.createClientFunction = createClientFunction
    this.loading = true
    this.nodesPath = this.zkClusterPath+"/nodes"
    this.sentCacheTime = 60 || sentCacheTime
    this.hostname = os.hostname()
    this.uid = 0
    this.sentCache = {}
    var self = this
    this.zkClient.connect(function(err){
        if(err){
            log.err("Error connecting to zookeeper ["+log.pretty(err)+"]",true)
            self.emit("error",err)
            return
        }
        self.nodeMonitor = new ZKMonitor(self.zkClient,self)
        self.nodeMonitor.pathMonitor(self.nodesPath)
    })
}

BigQueueClusterClient.prototype = new events.EventEmitter()

/**
 * Look for a node with specific id
 */
BigQueueClusterClient.prototype.getNodeById = function(id){
    for(var i=0; i<this.nodes.length; i++){
        if(this.nodes[i] && this.nodes[i].id == id)
            return this.nodes[i]     
    }
}
BigQueueClusterClient.prototype.shutdown = function(){
    this.nodeMonitor.running = false
}
/**
 * When node is added we0ll create a clusterNode object and if is loading we will notify this load
 */
BigQueueClusterClient.prototype.nodeAdded = function(node){
    var self = this
    var clusterNode = {}
    clusterNode["id"] = node.node
    clusterNode["data"] = JSON.parse(node.data)
    clusterNode["client"] = this.createClientFunction(clusterNode.data)
    clusterNode["sentMessages"] = {}
    clusterNode.client.on("error",function(){
        self.nodeError(clusterNode)
    })
    clusterNode.client.on("end",function(){
        self.nodeError(clusterNode)
    })
    this.nodes.push(clusterNode)
    if(this.loading)
        this.callReadyIfFinish()
    log.inf("Zookeeper node added ["+log.pretty(node)+"]")
}

/**
 * Remove expire messages from a clusterNode
 */
BigQueueClusterClient.prototype.expireMessagesSent = function(clusterNode){
    var self = this
    var actualTms = this.tms()
    var expiredIdx = []
    Object.keys(clusterNode).forEach(function(val,idx,arr){
        //Check if is expited
        if(val+self.sentCacheTime < actualTms)
            expiredIdx.push(idx)
    })
    for(i in expiredIdx){
        var idx = expired[i] 
        clusterNode.sentMessages.splice(idx,1)
    }
}

/**
 * Filter the nodes list by UP status
 */
BigQueueClusterClient.prototype.getNodesNoUp = function(){
    return this.nodes.filter(function(val){
        return val && val.data && val.data.status != "UP"
    })
}

/**
 * Check if all is loaded and emit the "ready" event
 */
BigQueueClusterClient.prototype.callReadyIfFinish = function(){
    if(this.loaded == undefined)
        this.loaded = 0
    this.loaded++;
    if(this.loaded >= this.nodeMonitor.monitoredPaths(this.nodesPath).length){
       this.loading = false
       this.emit("ready")
    }
}

/**
 * When a node is removed we'll remove it from the nodes list
 * and re-send all messages sent by this node
 */
BigQueueClusterClient.prototype.nodeRemoved = function(node){
    var clusterNode = this.getNodeById(node.node)
    this.nodes = this.nodes.filter(function(val){
        return val.id != clusterNode.id
    })
    if(clusterNode)
        this.resendMessages(clusterNode)
    log.inf("Zookeeper node removed ["+log.pretty(node)+"]")
}

/**
 * When a node data is changes we'll load it and if the node chage their status 
 * from up to any other we'll re-send their cached messages
 */
BigQueueClusterClient.prototype.nodeDataChange = function(node){
    var newData = JSON.parse(node.data)
    var clusterNode = this.getNodeById(node.node)
    var oldData = clusterNode.data
    clusterNode.data = newData

    if(oldData.status == "UP" && newData.status != oldData.status){
        this.resendMessages(clusterNode)
    }
    log.inf("Zookeeper node data chaged ["+log.pretty(node)+"]")
}

/**
 * Execute post to all cached messages messages
 */
BigQueueClusterClient.prototype.resendMessages = function(clusterNode){
    var sentMessages = clusterNode.sentMessages
    var tmss = Object.keys(sentMessages)
    var self = this
    log.inf("Resending messages of ["+log.pretty(clusterNode.data)+"]")
    for(var tms in tmss){
        var msgs = sentMessages[tmss[tms]]
        msgs.forEach(function(msg, index, array){
            self.postMessage(msg.topic,msg.data,function(err,key){
                if(err){
                    self.emit(err)
                    log.err("Error resending message ["+log.pretty(msg.data)+"] for topic ["+msg.topic+"]: "+log.pretty(err))
                }
            })
        })
    }
}

/**
 * If an error node found we'll notify it to zookeeper
 */
BigQueueClusterClient.prototype.nodeError = function(clusterNode){
    var self = this
    if(clusterNode.data.status == "UP"){
        clusterNode.data.errors++
        var nodePath = this.nodesPath+"/"+clusterNode.id
        this.zkClient.a_set(nodePath,JSON.stringify(clusterNode.data),-1,function(rc,error,stat){
            if(error){
                log.err("Error updating errors of ["+nodePath+"] in zookeeper")
            }
        })    
    }
}

/**
 * Call execute a function with all nodes and call callback when all execs are finished
 * the way to detect finishes are using a monitor that will be called by the exec function
 */
BigQueueClusterClient.prototype.withEveryNode = function(run,callback){
    var count = 0
    var total = this.nodes.length
    var hasError = false
    if(this.nodes.length == 0)
        callback()
    var monitor = function(err){
        if(!hasError && err){
            hasError = true;
            callback(err)
            return
        }
        count++
        if(count >= total)
            callback()
    }
    for(var i in this.nodes){
        run(this.nodes[i],monitor)
    }
}

/**
 * Get one node in a round-robin fashion
 */
BigQueueClusterClient.prototype.nextNodeClient = function(){
    var node = this.nodes.shift()
    this.nodes.push(node)
    return node
}

BigQueueClusterClient.prototype.generateClientUID = function(){
    return this.hostname+":"+(new Date().getTime())+":"+(this.uid++)
}

/**
 * Exec a function with one client if an error found we try with other client until
 * there are nodes into the list if the function fails with all nodes the callback will be
 * called with an error
 */
BigQueueClusterClient.prototype.withSomeClient = function(run,cb){
    var self = this
    var calls = 0
    var actualClient
    var monitor = function(err,data){
        calls++
        if(!err){
            cb(err,data)
            return
        }
        if(calls < self.nodes.length){
            self.nodeError(actualClient)
            actualClient = self.nextNodeClient()
            run(actualClient,monitor)
        }else{
            var error = err
            cb({err:"Node execution fail ["+error+"]"},null)
        }
    }
    if(this.nodes.length == 0){
        cb({err:"No nodes found"},null)
        return
    }
    actualClient = this.nextNodeClient()
    run(actualClient,monitor)
}

/**
 * Creates a topic into all nodes
 */
BigQueueClusterClient.prototype.createTopic = function(topic,callback){
    if(this.getNodesNoUp().length > 0){
        callback({err:"There are nodes down, try in few minutes"})
        return
    }

    var self = this
    var topicPath = this.zkClusterPath+topicsPath+"/"+topic
    var topicConsumerPath = this.zkClusterPath+topicsPath+"/"+topic+"/consumerGroups"
    //Check if exist in zookeeper
    this.zkClient.a_exists(topicPath,false,function(rc,error,stats){
            if(rc == ZK.ZNONODE){
                //If not exist create topic on each node
                self.withEveryNode(function(clusterNode,monitor){
                    clusterNode.client.createTopic(topic,function(err){
                        if(err){
                            monitor(JSON.stringify(err))
                        }else{
                            monitor()
                        }
                    })
                },
                function(err){
                    /*
                     *If there are any error running the create throw an error, 
                     *the orchestor should remove the unexistent topics from the redis that could create it
                     */
                    if(err){
                        log.err("Error creating topic ["+log.pretty(err)+"]")
                        callback(err)
                        return
                    }
                    self.zkClient.a_create(topicPath,"",self.zkFlags,function(rc, error, path){
                        if(rc!=0){
                            log.error("Creating path in zookeeper ["+topicPath+"], error: "+log.pretty(error))
                            callback({err: "Error registering topic ["+topic+"] into zookeeper"})
                            return
                        }
                        self.zkClient.a_create(topicConsumerPath,"",self.zkFlags,function(rc, error, path){
                            if(rc!=0){
                                log.error("Creating path in zookeeper ["+topicConsumerPath+"], error: "+log.pretty(error))
                                callback({err: "Error registering topic ["+topic+"] into zookeeper"})
                                return
                            }
                            callback()
                        })
                    })
                })
            }else{
                //If already exist throw error
                callback({err:"Error topic ["+topic+"] already exist"})
            }
    })
}

/**
 * Creates a consumer group into all nodes
 */
BigQueueClusterClient.prototype.createConsumerGroup = function(topic,consumer,callback){
    if(this.getNodesNoUp().length > 0){
        callback({err:"There are nodes down, try in few minutes"})
        return
    }
    var self = this
    var consumerPath  = this.zkClusterPath+topicsPath+"/"+topic+"/consumerGroups/"+consumer
    //Check if exist in zookeeper
    this.zkClient.a_exists(consumerPath,false,function(rc,error,stats){
            if(rc == ZK.ZNONODE){
                //If not exist create consumer on all nodes
                self.withEveryNode(function(clusterNode,monitor){
                    clusterNode.client.createConsumerGroup(topic,consumer,function(err){
                        if(err){
                            monitor(err)
                        }else{
                            monitor()
                        }
                    })
                },
                function(err){
                    /*
                     *If there are any error running the create throw an error, 
                     *the orchestor should remove the unexistent topics from the redis that could create it
                     */
                    if(err){
                        log.err("Error creating consumer group [topic:"+topic+"] [consumer:"+consumer+"], error: "+log.pretty(err))
                        callback(err)
                        return
                    }
                    self.zkClient.a_create(consumerPath,"",self.zkFlags,function(rc, error, path){
                        if(rc!=0){
                            log.error("Creating path in zookeeper ["+consumerPath+"], error: "+log.pretty(error))
                            callback({err: "Error registering consumer group ["+consumer+"] for topic ["+topic+"] into zookeeper"})
                            return
                        }
                        callback()
                    })
                })
            }else{
                //If already exist throw error
                callback({err:"Error consumer group ["+consumer+"] for topic ["+topic+"] already exist"})
            }
    })

}

/**
 * Get unix timestamp
 */
BigQueueClusterClient.prototype.tms = function(){
    var d = new Date().getTime()/1000
    return Math.floor(d)
}

/**
 * Post a message to one node generating a uid 
 */
BigQueueClusterClient.prototype.postMessage = function(topic,message,callback){
    var self = this
    var uid = this.generateClientUID()
    message["uid"] = uid
    this.withSomeClient(
        function(clusterNode,monitor){
                if(!clusterNode || clusterNode.data.status != "UP"){
                    monitor({err:"Error with clusterNode"},null)
                    return
                }
                clusterNode.client.postMessage(topic,message,function(err,key){
                if(!err){
                    //If no error, add the message to the cache
                    var t = self.tms()
                    key["uid"] = uid 
                    if(clusterNode.sentMessages[t] == undefined)
                        clusterNode.sentMessages[t] = []
                    clusterNode.sentMessages[t].push({"topic":topic,"data":message})
                }else{
                    log.err("Error posting message ["+log.pretty(err)+"] ["+log.pretty(clusterNode.data)+"]")
                }
                monitor(err,key)
                self.expireMessagesSent(clusterNode)
            })
        },
        function(err,key){
            callback(err,key)
        }
   )
}

/**
 * Return one message queued for a consumer group, this method will generates a recipientCallback 
 * that we'll be used 
 */
BigQueueClusterClient.prototype.getMessage = function(topic,group,visibilityWindow,callback){
    var self = this
    this.withSomeClient(
        function(clusterNode,monitor){     
            if(!clusterNode || clusterNode.data.status != "UP"){
                monitor({err:"Cluster node error or down"})
                return
            }
            clusterNode.client.getMessage(topic,group,visibilityWindow,function(err,data){
                if(err){
                    monitor(err,undefined)
                }else{
                    if(data.id){
                        var recipientCallback = self.encodeRecipientCallback({"nodeId":clusterNode.id,"topic":topic,"consumerGroup":group,"id":data.id})
                        data["recipientCallback"] = recipientCallback
                        monitor(undefined,data)
                    }else{
                        monitor(undefined,undefined)
                    }
                }
            })
        },
        function(err,data){
            if(data == {} || err)
                data = undefined
            if(err)
                log.err("Error getting messages ["+log.pretty(err)+"]")
            callback(err,data)
        }
    )
}

/**
 * Generates the recipient callback to return at get instance
 */
BigQueueClusterClient.prototype.encodeRecipientCallback = function(data){
    var keys = Object.keys(data)
    var strData = ""
    for(var i in keys){
        strData = strData+":"+keys[i]+":"+data[keys[i]]
    }
    return strData.substring(1)
}

/**
 * Regenerate the data ecoded by #encodeRecipientCallback
 */
BigQueueClusterClient.prototype.decodeRecipientCallback = function(recipientCallback){
   var splitted = recipientCallback.split(":")
   var data = {}
   for(var i = 0; i< splitted.length; i=i+2){
        data[splitted[i]] = splitted[i+1]
   }
   return data
}

BigQueueClusterClient.prototype.ackMessage = function(topic,group,recipientCallback,callback){
    var repData = this.decodeRecipientCallback(recipientCallback)
    var node = this.getNodeById(repData.nodeId)
    if(!node || node.data.status != "UP"){
        callback({err:"Node ["+repData.nodeId+"] not found or down"})
        return
    }
    node.client.ackMessage(topic,group,repData.id,function(err){
        if(err)
            log.err("Error doing ack of recipientCallback ["+recipientCallback+"], error: "+log.pretty(err))
        callback(err)
    })
}

BigQueueClusterClient.prototype.failMessage = function(topic,group,recipientCallback,callback){
    var repData = this.decodeRecipientCallback(recipientCallback)
    var node = this.getNodeById(repData.nodeId)
    if(!node || node.data.status != "UP"){
        callback({err:"Node ["+repData.nodeId+"] not found or down"})
        return
    }
    node.client.failMessage(topic,group,repData.id,function(err){
        if(err)
            log.err("Error doing fail of recipientCallback ["+recipientCallback+"], error: "+log.pretty(err))
        callback(err)
    })
}

BigQueueClusterClient.prototype.listTopics = function(callback){
    var topicsPath = this.zkClusterPath+"/topics"
    this.zkClient.a_get_children(topicsPath,false,function(rc,error,children){
        if(rc!=0){
            log.err("Error listing topics ["+log.pretty(error)+"]")
        }
        callback(children)
    })
}

BigQueueClusterClient.prototype.getConsumerGroups = function(topic,callback){
    var consumerGroupsPath = this.zkClusterPath+"/topics/"+topic+"/consumerGroups"
    this.zkClient.a_get_children(consumerGroupsPath,false,function(rc,error,children){
        if(rc!=0){
            log.err("Error getting consumer groups ["+log.pretty(error)+"]")
            callback({err:"Error ["+rc+"-"+error+", "+consumerGroupsPath+"]"},undefined)
            return
        }
        callback(undefined,children)
    })
}


exports.createClusterClient = function(clusterConfig){
    var zk = new ZK(clusterConfig.zkConfig)
    return  new BigQueueClusterClient(zk,clusterConfig.zkClusterPath,
                                            clusterConfig.zkFlags,
                                            clusterConfig.createNodeClientFunction,
                                            clusterConfig.sentCacheTime)
} 
