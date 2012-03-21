var events = require("events"),
    ZK = require("zookeeper"),
    ZKMonitor = require("../lib/zk_monitor.js")

var ADDED = "ADDED"
var REMOVED = "REMOVED"
var DATACHANGE = "DATACHANGE"

function BigQueueClusterOrchestrator(zkClient,zkClustersPath,createNodeClientFunction,checkInterval){
    this.zkClient = zkClient
    this.zkClustersPath = zkClustersPath
    this.createNodeClientFunction = createNodeClientFunction
    this.checkInterval = checkInterval
    this.clusters = {}
    var self = this
    this.zkRules = [
        {regexPath: new RegExp(self.zkClustersPath+"$"), controller: self.clustersController},
        {regexPath: new RegExp(self.zkClustersPath+"/(\\w+)$"), controller: self.createMonitorPath},
        {regexPath: new RegExp(self.zkClustersPath+"/(.*)/topics$"), controller: self.topicsController},
        {regexPath: new RegExp(self.zkClustersPath+"/(.*)/topics/(.*)/consumerGroups$"), controller: self.consumerGroupsController},
        {regexPath: new RegExp(self.zkClustersPath+"/(.*)/nodes$"), controller: self.nodesController},
                
    ]
    this.nodeMonitor = new ZKMonitor(self.zkClient,self)
        
    this.init()

}

BigQueueClusterOrchestrator.prototype = new events.EventEmitter()

BigQueueClusterOrchestrator.prototype.nodeAdded = function(node){
    this.applyRules(ADDED,node)
}
BigQueueClusterOrchestrator.prototype.nodeDataChange = function(node){
    this.applyRules(DATACHANGE,node)
}
BigQueueClusterOrchestrator.prototype.nodeRemoved = function(node){
    this.applyRules(REMOVED,node)
}

BigQueueClusterOrchestrator.prototype.applyRules = function(type,node){
    var self = this
    this.zkRules.forEach(function(rule){
        if(rule.regexPath.test(node.path)){
            rule.controller(node,type,rule,self)
        }
    })
}

BigQueueClusterOrchestrator.prototype.createMonitorPath = function(node,type,rule,context){
    if(type == ADDED)
        context.nodeMonitor.pathMonitor(node.path+"/"+node.node)
}

BigQueueClusterOrchestrator.prototype.clustersController = function(node,type,rule,context){
    if(type == ADDED){
        context.clusters[node.node] = {"data":node.data}
        var clusterPath = node.path+"/"+node.node
        context.nodeMonitor.pathMonitor(clusterPath)
    }
}

BigQueueClusterOrchestrator.prototype.topicsController = function(node,type,rule,context){
    var cluster = node.path.match(/.*\/(.*)\/topics$/)[1]
    if(type = ADDED){
        if(context.clusters[cluster]["topics"] == undefined)
            context.clusters[cluster]["topics"] = {}
        context.clusters[cluster]["topics"][node.node] = {"data":node.data}
        context.nodeMonitor.pathMonitor(node.path+"/"+node.node+"/consumerGroups")
    }

}

BigQueueClusterOrchestrator.prototype.consumerGroupsController = function(node,type,rule,context){
    var pathData = node.path.match(/.*\/(.*)\/topics\/(.*)\/consumerGroups$/)
    var cluster = pathData[1]
    var topic = pathData[2]
    if(type == ADDED){
        if(context.clusters[cluster]["topics"][topic]["consumerGroups"] == undefined){
            context.clusters[cluster]["topics"][topic]["consumerGroups"] = []
        }
        context.clusters[cluster]["topics"][topic]["consumerGroups"].push(node.node)
   }
}

BigQueueClusterOrchestrator.prototype.nodesController = function(node,type,rule,context){
    var cluster = node.path.match(/.*\/(.*)\/nodes$/)[1]
    if(type == ADDED){
        if(context.clusters[cluster]["nodes"] == undefined)
            context.clusters[cluster]["nodes"] = {}
        context.clusters[cluster]["nodes"][node.node] = JSON.parse(node.data)
    }
    if(type == DATACHANGE){
        var newData = JSON.parse(node.data)
        context.healthCheckNode(cluster,node.node,newData,function(err){

        })
    }
}

BigQueueClusterOrchestrator.prototype.healthCheckNode = function(clusterId,nodeId,data,callback){
    try{
        var client = this.createNodeClientFunction(data)
    }catch(e){
        console.log("catch")
    }
    var zkNodePath = this.zkClustersPath+"/"+clusterId+"/nodes/"+nodeId
    var self = this
    client.on("ready",function(){
        if(data.status != "UP"){
            data.status = "UP"
            data.errors = 0
            self.clusters[clusterId]["nodes"][nodeId] = data
            self.zkClient.a_set(zkNodePath,JSON.stringify(data),-1,function(rc, error, stat){
                if(rc!=0){
                    self.processingError(error)
                    return
                }
                callback()
            })
            client.shutdown()
        }
    })

    client.on("error",function(err){
        if(data.status != "DOWN"){
            data.status = "DOWN"
            self.clusters[clusterId]["nodes"][nodeId] = data
            self.zkClient.a_set(zkNodePath,JSON.stringify(data),-1,function(rc, error, stat){
                if(rc!=0){
                    self.processingError(error)
                    return
                }
               callback(err)
            })
        }
    })
}

BigQueueClusterOrchestrator.prototype.periodicalCheck = function(context){
    var clusterKeys = Object.keys(context.clusters)
    for(var i in clusterKeys){
        var clusterKey = clusterKeys[i]
        var cluster = context.clusters[clusterKey]
        var nodesKeys = Object.keys(cluster["nodes"])
        for(var j in nodesKeys){
            var nodeKey = nodesKeys[j]
            context.healthCheckNode(clusterKey,nodeKey,context.clusters[clusterKey]["nodes"][nodeKey],function(err){
            })
        }
    }
}

BigQueueClusterOrchestrator.prototype.processingError = function(error){
    console.trace("Error processing ["+error+"]")
    process.exit(1)
}
BigQueueClusterOrchestrator.prototype.shutdown = function(){
    clearInterval(this.intervalId)
    this.nodeMonitor.running = false
}
BigQueueClusterOrchestrator.prototype.init = function(){
    var self = this
    this.zkClient.connect(function(err){
        if(err){
            self.emit("error",err)
            return
        }
        self.nodeMonitor.pathMonitor(self.zkClustersPath)
        setTimeout(function(){
            self.intervalId = setInterval(function(){
                self.periodicalCheck(self)
            },self.checkInterval)
        },self.checkInterval)
        self.emit("ready")
    })

}

exports.createOrchestrator = function(conf){
   var zk = new ZK(conf.zkConfig)
   var orch = new BigQueueClusterOrchestrator(zk,conf.zkClustersPath,conf.createNodeClientFunction,conf.checkInterval)
   return orch
}
