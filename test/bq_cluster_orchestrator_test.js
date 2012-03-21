var should = require("should"),
    ZK = require("zookeeper"),
    oc = require("../lib/bq_cluster_orchestrator.js"),
    bq = require("../lib/bq_client.js")

describe("Orchestrator",function(){
    var clusterPath = "/bq/clusters/test"
    
    var redisClient1

    var redisClient2

    var zkConfig = {
            connect: "localhost:2181",
            timeout: 200000,
            debug_level: ZK.ZOO_LOG_LEVEL_WARN,
            host_order_deterministic: false
        }   

    var ocConfig = {
        "zkClustersPath":"/bq/clusters",
        "zkConfig":zkConfig,
        "createNodeClientFunction":bq.createClient,
        "checkInterval":500
    }
    var zk = new ZK(zkConfig)

    before(function(done){
         zk.connect(function(err){
            if(err){
                done(err)
            }else{
                done()  
            }
        })
    });
    /**
      * Delete recursively all 
      */ 
    var deleteAll = function(zk,path,cb,c){
        c = c || 0
        zk.a_get_children(path,false,function(rc,error,children){
            var total = 0
            if(children)
                total = children.length
            var count = 0
            if(total == 0){
                if(c>0){
                    zk.a_delete_(path,-1,function(rc,error){
                        cb()
                    })
                }else{
                    cb()
                }
            }
            for(var i in children){
                var p = path+"/"+children[i]
                deleteAll(zk,p,function(){
                    count++;
                    if(count >= total){
                        zk.a_delete_(p,-1,function(rc,error){
                            cb()
                        })
                     }
                },c++)
            }
        })

    }

    beforeEach(function(done){
        zk.a_create("/bq","",0,function(rc,error,path){    
            zk.a_create("/bq/clusters","",0,function(rc,error,path){
                zk.a_create("/bq/clusters/test","",0,function(rc,error,path){
                   deleteAll(zk,"/bq/clusters/test",function(){
                       zk.a_create("/bq/clusters/test/topics","",0,function(rc,error,path){
                            zk.a_create("/bq/clusters/test/nodes","",0,function(rc,error,path){
                                zk.a_delete_("/bq/clusters/test/nodes/redis1",-1,function(){
                                    zk.a_delete_("/bq/clusters/test/nodes/redis2",-1,function(){
                                        zk.a_create("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"UP"}),0,function(rc,error,path){
                                            zk.a_create("/bq/clusters/test/nodes/redis2",JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"UP"}),0,function(rc,error,path){
                                                done()
                                            })
                                        })
                                    })
                                })
                            })
                        })
                    })
                })
            })
        })
    }) 

    it("Should check nodes status when any event is produced",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
        setTimeout(function(){
            zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":1,"status":"DOWN"}),-1,function(){
                setTimeout(function(){
                    zk.a_get("/bq/clusters/test/nodes/redis1",false,function(rc,error,stat,data){
                       should.exist(rc)
                       rc.should.equal(0)
                       should.exist(data)
                       var d = JSON.parse(data)
                       d.status.should.equal("UP")
                       d.errors.should.equal(0)
                       done()
                       orch.shutdown()
                    })
                },100)
            })
        },200)
        })
    })
    it("Should check nodes periodically looking for connection problems",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
            setTimeout(function(){
                zk.a_create("/bq/clusters/test/nodes/redis3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP"}),0,function(rc,error,path){
                    setTimeout(function(){
                        zk.a_get("/bq/clusters/test/nodes/redis3",false,function(rc,error,stat,data){
                           should.exist(rc)
                           rc.should.equal(0)
                           should.exist(data)
                           var d = JSON.parse(data)
                           d.status.should.equal("DOWN")
                           done()
                           orch.shutdown()
                        })
                    },1000)
                })
            },200)
        })
    })
    it("Should check nodes periodically looking for inconsistencies and sync if one is found")
    it("Should sync new nodes")
    it("Should sync nodes if the status will be change to UP")
    it("Should not sync nodes in FORCEDOWN status")
    it("should detect new nodes")
    it("should detect new topics")
    it("should detect new clusters")

})
