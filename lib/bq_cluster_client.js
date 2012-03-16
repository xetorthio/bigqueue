var redis = require('redis'),
    fs = require('fs'),
    should = require('should'),
    events = require("events"),
    ZK = require ("zookeeper"),
    ZKMonitor = require("../lib/zk_monitor.js")

var topicsPath = "/topics"

/**
 * Object responsible for mantains updated the list of cluster nodes
 */
function ClientPool(zkClient,nodesPath,createClientFunction){
    this.zkMonitor = new ZKMonitor(zkClient,this)
    this.zkMonitor.pathMonitor(nodesPath)
}

ClientPool.prototype.nodeAdded = function(node){
    console.log(node)
}
ClientPool.prototype.nodeRemoved = function(path,node,data){
    console.log("removed")
}


/**
 *  Object responsible for execute all commands throght the bq cluster
 */
function BigQueueClusterClient(zkClient,zkClusterPath,zkFlags,createClientFunction){

    this.nodes=[]

    this.zkClient = zkClient
    this.zkClusterPath = zkClusterPath
    this.zkFlags = 0 | zkFlags
    this.createClientFunction = createClientFunction
    this.loading = true
    this.nodesPath = this.zkClusterPath+"/nodes"
    var self = this
    this.zkClient.connect(function(err){
        if(err){
            self.emit("error",err)
            return
        }
        self.nodeMonitor = new ZKMonitor(self.zkClient,self)
        self.nodeMonitor.pathMonitor(self.nodesPath)
    })
}

BigQueueClusterClient.prototype = new events.EventEmitter()

/**
 * When node added 
 */
BigQueueClusterClient.prototype.nodeAdded = function(node){
    var self = this
    var clusterNode = {}
    clusterNode["id"] = node.node
    clusterNode["data"] = JSON.parse(node.data)
    clusterNode["client"] = this.createClientFunction(clusterNode.data)
    clusterNode.client.on("error",function(){
        self.nodeError(clusterNode)
    })
    clusterNode.client.on("end",function(){
        self.nodeError(clusterNode)
    })
    this.nodes.push(clusterNode)
    if(this.loading)
        this.callReadyIfFinish()
}

BigQueueClusterClient.prototype.getNodesNoUp = function(){
    return this.nodes.filter(function(val){
        return val.data.status != "UP"
    })
}

BigQueueClusterClient.prototype.callReadyIfFinish = function(){
    if(this.loaded == undefined)
        this.loaded = 0
    this.loaded++;
    if(this.loaded >= this.nodeMonitor.monitoredPaths(this.nodesPath).length){
        this.emit("ready")
        this.loading = false
    }
}

BigQueueClusterClient.prototype.nodeRemoved = function(node){
}
BigQueueClusterClient.prototype.nodeDataChange = function(node){
}
BigQueueClusterClient.prototype.nodeError = function(clusterNode){
}
BigQueueClusterClient.prototype.withEveryNode = function(run,callback){
    var count = 0
    var total = this.nodes.length
    var hasError = false
    if(this.nodes.length == 0)
        callback()
    var monitor = function(err){
        if(!hasError && err){
            callback(err)
            hasError = true;
        }
        count++
        if(count >= total)
            callback()
    }
    for(i in this.nodes){
        run(this.nodes[i],monitor)
    }
}

BigQueueClusterClient.prototype.createTopic = function(topic,callback){
    if(this.getNodesNoUp().length > 0){
        callback({err:"There are nodes down, try in few minutes"})
        return
    }

    var self = this
    var topicPath = this.zkClusterPath+topicsPath+"/"+topic
    //Check if exist in zookeeper
    this.zkClient.a_exists(topicPath,false,function(rc,error,stats){
            if(rc == ZK.ZNONODE){
                //If not exist create topic on each node
                self.withEveryNode(function(clusterNode,monitor){
                    clusterNode.client.createTopic(topic,function(err){
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
                        callback(err)
                        return
                    }
                    self.zkClient.a_create(topicPath,"",self.zkFlags,function(rc, error, path){
                        if(rc!=0){
                            callback({err: "Error registering topic ["+topic+"] into zookeeper"})
                        }
                        callback()
                    })
                })
            }else{
                //If already exist throw error
                callback({err:"Error topic ["+topic+"] already exist"})
            }
    })
}

exports.createClusterClient = function(clusterConfig){
    var zk = new ZK(clusterConfig.zkConfig)
    return  new BigQueueClusterClient(zk,clusterConfig.zkClusterPath,
                                            clusterConfig.zkFlags,
                                            clusterConfig.createClientFunction)
} 
