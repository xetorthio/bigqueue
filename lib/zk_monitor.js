var ZK = require("zookeeper"),
    events = require("events")

/**
 * It's a simple way to monitor a zk path 
 * removing the zookeeper logic from the main code
 * at the start the nodeAdded method will be called one time per child node
 */

exports = module.exports = ZKMonitor
function ZKMonitor(zkClient,callable){
    events.EventEmitter.call(this);
    this.zkClient = zkClient
    this.callable = callable
    this.actualPath = {}
}

ZKMonitor.prototype = new events.EventEmitter();

ZKMonitor.prototype.pathMonitor = function(path){
    var self = this
    this.first = true
    this.zkClient.a_exists(path,false,function(rc,err,stat){
        if(rc!=0){
            self.emit("error","Path dosn't exist ["+path+"]")
            return;
        }
        if(self.actualPath[path] == undefined){
            self.actualPath[path] = []
        }
        var onData = function(rc,error,childrens){
            if(childrens)
                self.updateChilds(path,childrens)
        }
        var onEvent = function(type,state,path){
            self.zkClient.aw_get_children(path,onEvent,onData)
        }
        self.zkClient.aw_get_children(path,onEvent,onData)
         
    })
}


ZKMonitor.prototype.updateChilds = function(path,childrens){
   
    var onGetEvent = function(type,state,path){
    }

    var pathData = this.actualPath[path] 
    var added = childrens.filter(function(element,index,array){
        return pathData.indexOf(element) < 0
    })
    
    var removed = pathData.filter(function(element,index,array){
        return childrens.indexOf(element) < 0
    })
    addAll(pathData,added)
    removeAll(pathData,removed)
    for(var r in removed){
        this.callable.nodeRemoved({"path":path, "node":removed[r]})
    }
    for(var a in added){
        new MonitoreableNode(this.zkClient,path,added[a],this.callable)
    }
}

ZKMonitor.prototype.monitoredPaths = function(path){
    return this.actualPath[path]
}

/**
 * Wraps per child get data logic
 */
function MonitoreableNode(zkClient,path,node,callable){
    this.zkClient = zkClient
    this.path = path
    this.node = node
    this.callable = callable
    var self = this
    var onGetEvent = function(type,state,path){
        self.zkClient.aw_get(self.path+"/"+self.node,onGetEvent,function(rc,error,stat,data){
            if(type == ZK.ZOO_CHANGED_EVENT)
                self.callable.nodeDataChange({"path":self.path,"node":self.node,"data":data})
        })
    }
    this.zkClient.aw_get(this.path+"/"+this.node,onGetEvent,function(rc,error,stat,data){
       self.callable.nodeAdded({"path":self.path,"node":self.node,"data":data})
    })

}

addAll = function(orig, arr){
    for(var i in arr){
       orig.push(arr[i])
    }
}

removeAll = function(orig,remove){
    var idx = []
    for(var i in orig){
        if(remove.indexOf(orig[i])>=0){
            idx.push(i)
        }
    }
    for(var i in idx){
        orig.splice(idx[i],1)
    }
}

