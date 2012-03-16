var should = require('should'),
    redis = require('redis'),
    ZKMonitor = require('../lib/zk_monitor.js'),
    ZK = require("zookeeper")

describe("Zookeeper Monitor",function(){
    var zk = new ZK({
        connect: "localhost:2181",
        timeout: 200000,
        debug_level: ZK.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false,
        data_as_buffer:false
    });

    describe("Path Monitor",function(){
        var zk; 
        beforeEach(function(done){
            zk = new ZK({
                connect: "localhost:2181",
                timeout: 200000,
                debug_level: ZK.ZOO_LOG_LEVEL_WARN,
                host_order_deterministic: false,
                data_as_buffer:false
            });
            zk.connect(function(err){
                zk.a_create("/test","", 0,function (rc, error, path)  {
                    zk.a_get_children("/test",false,function(rc,error,children){
                        var count = 0;
                        var finish = children.length
                        if(finish==0){
                            done()
                            return;
                        }
               
                        for(var i in children){
                            zk.a_delete_("/test/"+children[i],-1,function(rc,error,path){
                                count++;
                                if(count>=finish){
                                    done()
                                }
                            })
                        }
                    })
                })
            }) 
        })
        
        afterEach(function(){
            zk.close()
        })
        it("should get an error if monitor path doesn't exist",function(done){
            var monitor = new ZKMonitor(zk,null) 
            monitor.on("error",function(){
                done()
            })
            zk.a_delete_("/no-exist-path",-1,function(rc,error){
                monitor.pathMonitor("/no-exist-path")
            })
        })
        it("should notify on each child data change",function(done){
            var callable = {}
            callable.nodeRemoved = function(obj){}
            callable.nodeDataChange = function(obj){
                should.exist(obj)
                obj.path.should.equal("/test")
                obj.node.should.equal("1")
                obj.data.should.equal("data-modif")
                done()
            }
            callable.nodeAdded = function(){}
            callable.initialLoad = function(initialNodes){
                zk.a_set("/test/1","data-modif",-1,function(rc,error,stat){ })
            }
            zk.a_create("/test/1","data", 0,function (rc,error,path){
                var monitor = new ZKMonitor(zk,callable)
                monitor.pathMonitor("/test") 
            })

        })
        it("should notify on each child add",function(done){
            var callable = {}
            callable.nodeRemoved = function(obj){}
            callable.nodeDataChange = function(obj){}
            callable.nodeAdded = function(obj){
                should.exist(obj)
                obj.path.should.equal("/test")
                obj.node.should.equal("1")
                obj.data.should.equal("data")
                done()
            }
            var monitor = new ZKMonitor(zk,callable) 
            monitor.pathMonitor("/test")
                zk.a_create("/test/1","data", 0,function (rc,error,path){
            })
            
        })
        it("should notify on each child remove",function(done){
            var callable = {}
            callable.nodeRemoved = function(obj){
                should.exist(obj)
                obj.path.should.equal("/test")
                obj.node.should.equal("1")
                done()
            }
            callable.nodeDataChange = function(obj){}
            callabke.nodeAdded = function(obj){}
            callable.initialLoad = function(initialNodes){
                zk.a_delete_("/test/1",-1,function(rc, error){})
            }
            zk.a_create("/test/1","data", 0,function (rc,error,path){
               var monitor = new ZKMonitor(zk,callable) 
               monitor.pathMonitor("/test")
            })

        })
        it("should call initialLoad when start",function(done){
            var callable = {}
            callable.nodeRemoved = function(obj){}
            callable.nodeDataChange = function(obj){}
            callabke.nodeAdded = function(obj){}
            callable.initialLoad = function(initialNodes){
                should.exist(initialNodes)
                initialNodes.should.have.length(2)
                initialNodes[0].should.contains.keys("node","path","data")
            }
            zk.a_create("/test/1","data", 0,function (rc,error,path){
                zk.a_create("/test/2","data", 0,function (rc,error,path){
                   var monitor = new ZKMonitor(zk,callable) 
                   monitor.pathMonitor("/test")
                })
            })
        })
    })
})
