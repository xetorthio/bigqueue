var should = require('should'),
    redis = require('redis'),
    ZK = require('zookeeper'),
    bq = require('../lib/bq_client.js'),
    bqc = require('../lib/bq_cluster_client.js')

var j = 0
describe("Big Queue Cluster",function(){
    
    //Prepare stage
    var clusterPath = "/bq/clusters/test"

    var zkConfig = {
            connect: "localhost:2181",
            timeout: 200000,
            debug_level: ZK.ZOO_LOG_LEVEL_WARN,
            host_order_deterministic: false
        }   

    var bqClientConfig = {
        "zkConfig":zkConfig,
        "zkClusterPath":clusterPath,
        "createNodeClientFunction":bq.createClient
    }

    var zk = new ZK(zkConfig)

    var bqClient
    var redisClient1
    var redisClient2

    before(function(done){
        redisClient1 = redis.createClient(6379,"127.0.0.1")
        redisClient1.on("ready",function(){
            redisClient2= redis.createClient(6380,"127.0.0.1")
            redisClient2.on("ready",function(){
                done()
            })
        })
    }) 

    before(function(done){
         zk.connect(function(err){
            if(err){
                done(err)
            }else{
                done()  
            }
        })
    });
    
    beforeEach(function(done){
        redisClient1.flushall(function(err,data){
            redisClient2.flushall(function(err,data){
                done()
            })
        })
    })
   
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
                                zk.a_create("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"UP"}),0,function(rc,error,path){
                                    zk.a_create("/bq/clusters/test/nodes/redis2",JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"UP"}),0,function(rc,error,path){
                                        bqClient = bqc.createClusterClient(bqClientConfig)
                                        bqClient.on("ready",function(){
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

    afterEach(function(){
        bqClient.shutdown()
    })

    //End of prepare stage 

    describe("#createTopic",function(){
        it("should register the topic after creation and create the consumer node",function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                zk.a_exists(clusterPath+"/topics/testTopic",false,function(rc,error,stat){
                    zk.a_exists(clusterPath+"/topics/testTopic/consumerGroups",false,function(rc,error,stat){
                        if(rc!=0){
                            done(rc+"-"+error)
                        }else{
                            done()
                        }
                    })
                })
            })
        })
        it("should propagate the create throught all redis",function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                redisClient1.sismember("topics","testTopic",function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    redisClient2.sismember("topics","testTopic",function(err,data){
                        should.not.exist(err)
                        data.should.equal(1)
                        done()
                    })
                })
            })
        })
        it("should fail if there are any redis with problems",function(done){
            zk.a_set("/bq/clusters/test/nodes/redis2",JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"DOWN"}),-1,function(rc, err,stat){
                rc.should.equal(0)
                var client = bqc.createClusterClient(bqClientConfig)
                client.once("ready",function(){
                    client.createTopic("testTopic",function(err){
                        should.exist(err)
                        done()            
                    })
                })
            })
        })
        it("should fail if the topic already exist",function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                bqClient.createTopic("testTopic",function(err){
                    should.exist(err)
                    done()
                })
            })
        })
    })

    describe("#createConsumer",function(done){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                done()
            })
        })
        it("should register the consumer group after creation",function(done){
            bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                should.not.exist(err)
                zk.a_exists(clusterPath+"/topics/testTopic/consumerGroups/testConsumer",false,function(rc,error,stat){
                    if(rc!=0){
                        done(rc+"-"+error)
                    }else{
                        done()
                    }
                })
            })
        })
        it("should fail if some registered server isn't up",function(done){
            zk.a_set("/bq/clusters/test/nodes/redis2",JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"DOWN"}),-1,function(rc, err,stat){
                rc.should.equal(0)
                var client = bqc.createClusterClient(bqClientConfig)
                client.once("ready",function(){
                    client.createConsumerGroup("testTopic","testConsumer",function(err){
                        should.exist(err)
                        done()            
                    })
                })
            })
        })
        it("should propagate the creation through all nodes",function(done){
            bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                should.not.exist(err)
                redisClient1.sismember("topics:testTopic:consumers","testConsumer",function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    redisClient2.sismember("topics:testTopic:consumers","testConsumer",function(err,data){
                        should.not.exist(err)
                        data.should.equal(1)
                        done()
                    })
                })
            })
        })
        it("should fail if any redis get an error on create",function(done){
            redisClient2.del("topics",function(err,data){
                should.not.exist(err)
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    should.exist(err)
                    done()            
                })
            })
        })
    })

    describe("#postMessage",function(){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                done()
            })
        })
        it("should balance the writes",function(done){
            bqClient.postMessage("testTopic",{msg:"test1"},function(err,key){
                bqClient.postMessage("testTopic",{msg:"test2"},function(err,key){
                    bqClient.postMessage("testTopic",{msg:"test3"},function(err,key){
                        bqClient.postMessage("testTopic",{msg:"test4"},function(err,key){
                            redisClient1.get("topics:testTopic:head",function(err,data){
                                should.not.exist(err)
                                should.exist(data)
                                data.should.equal(""+2)
                                redisClient2.get("topics:testTopic:head",function(err,data){
                                    should.not.exist(err)
                                    should.exist(data)
                                    data.should.equal(""+2)
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })
        it("should try to resend the message to another node if an error ocurrs sending",function(done){
            zk.a_create("/bq/clusters/test/nodes/redis3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP"}),0,function(rc,error,path){
                bqClient = bqc.createClusterClient(bqClientConfig)
                bqClient.on("ready",function(){
                    bqClient.postMessage("testTopic",{msg:"test1"},function(err,key){
                        bqClient.postMessage("testTopic",{msg:"test2"},function(err,key){
                            bqClient.postMessage("testTopic",{msg:"test3"},function(err,key){
                                bqClient.postMessage("testTopic",{msg:"test4"},function(err,key){
                                    redisClient1.get("topics:testTopic:head",function(err,data1){
                                        should.not.exist(err)
                                        should.exist(data1)
                                        redisClient2.get("topics:testTopic:head",function(err,data2){
                                            should.not.exist(err)
                                            should.exist(data2)
                                            var sum = parseInt(data1)+parseInt(data2)
                                            sum.should.equal(4)
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
        it("should notify an error to zookeeper on node error",function(done){
            zk.a_create("/bq/clusters/test/nodes/redis3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP"}),0,function(rc,error,path){
                var oldData
                zk.aw_get("/bq/clusters/test/nodes/redis3",function(type,state,path){
                    zk.a_get(path,false,function(rc,error,stat,data){
                        var newData = JSON.parse(data)
                        newData.host.should.equal(oldData.host)
                        newData.port.should.equal(oldData.port)
                        done()
                    }) 
                },
                function (rc,error,stat,data){
                    oldData = JSON.parse(data)
                })
                bqClient = bqc.createClusterClient(bqClientConfig)
                bqClient.on("ready",function(){
                    bqClient.postMessage("testTopic",{msg:"test1"},function(err,key){
                        bqClient.postMessage("testTopic",{msg:"test2"},function(err,key){
                            bqClient.postMessage("testTopic",{msg:"test3"},function(err,key){
                            })
                        })
                    })
                })
            })
        })
        it("should cache the data and resend it if a node goes down",function(done){
            var self = this
            bqClient.postMessage("testTopic",{msg:"test1"},function(err,key){
                bqClient.postMessage("testTopic",{msg:"test2"},function(err,key){
                    redisClient2.get("topics:testTopic:head",function(err,data){
                        var oldHead = parseInt(data)
                        should.not.exist(err)
                        should.exist(key)
                        zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"DOWN"}),-1,function(rc,error,stat){
                            rc.should.equal(0)
                            var check =function(){
                                redisClient2.get("topics:testTopic:head",function(err,data){
                                    var newHead = parseInt(data)
                                    if(newHead != oldHead){
                                        newHead.should.equal(oldHead + 1) 
                                        done()
                                    }else{
                                        process.nextTick(check)
                                    }
                                })
                            }
                            check()
                        })
                    })
                })
            })
        })
        it("should cache the data and resend it if a node is removed",function(done){
            var self = this
            bqClient.postMessage("testTopic",{msg:"test1"},function(err,key){
                bqClient.postMessage("testTopic",{msg:"test2"},function(err,key){
                    redisClient2.get("topics:testTopic:head",function(err,data){
                        var oldHead = parseInt(data)
                        should.not.exist(err)
                        should.exist(key)
                        zk.a_delete_("/bq/clusters/test/nodes/redis1",-1,function(rc,error,stat){
                            rc.should.equal(0)
                            var check =function(){
                                redisClient2.get("topics:testTopic:head",function(err,data){
                                    var newHead = parseInt(data)
                                    if(newHead != oldHead){
                                        newHead.should.equal(oldHead + 1) 
                                        done()
                                    }else{
                                        process.nextTick(check)
                                    }
                                })
                            }
                            check()
                        })
                    })
                })
            })

        })
    }) 

    describe("#getMessage",function(){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testGroup",function(err){
                    should.not.exist(err)
                    done()

                })
            })
        })

        it("should generate and add a recipientCallback to the returned message",function(done){
            //2 post because get message using round-robin
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key){
                    bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                        should.not.exist(err)
                        should.exist(data)
                        data.should.have.property("uid")
                        data.should.have.property("recipientCallback")
                        done()
                    })
                })
           })
        })
        it("should balance the gets throw all nodes",function(done){
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,data){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,data){
                    bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                            should.not.exist(err)
                            should.exist(data)
                            data.should.have.property("uid")
                            data.should.have.property("recipientCallback")
                        bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                            should.not.exist(err)
                            should.exist(data)
                            data.should.have.property("uid")
                            data.should.have.property("recipientCallback")
                            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                                should.not.exist(err)
                                should.not.exist(data)
                                done()
                            })
                        })
                    })
                })
           })

        })
        it("should run ok if a node is down",function(done){
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,data){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,data){
                    zk.a_set("/bq/clusters/test/nodes/redis2",JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"DOWN"}),-1,function(rc, err,stat){
                        rc.should.equal(0)
                         bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                            should.not.exist(err)
                            should.exist(data)
                            data.should.have.property("uid")
                            data.should.have.property("recipientCallback")
                            done()
                        })
                    })
           
                })
            })
        })

        it("should run ok if a all nodes are down",function(done){
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,data){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,data){
                    zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"DOWN"}),-1,function(rc, err,stat){
                        zk.a_set("/bq/clusters/test/nodes/redis2",JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"DOWN"}),-1,function(rc, err,stat){
                            rc.should.equal(0)
                            setTimeout(function(){
                                bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                                    should.not.exist(data)
                                    done()
                                })
                            },100)
                        })
                    })
                })
            })
        }) 

        it("should get the uid generated at post instance",function(done){
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key2){
                    var uid = key.uid
                    bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                        should.not.exist(err)
                        should.exist(data)
                        data.uid.should.equal(uid)
                        done()
                    })
                })
           })

        })
        it("should return undefined if no message found",function(done){
            redisClient1.set("topics:testTopic:head",0,function(err,data){
                redisClient2.set("topics:testTopic:head",0,function(err,data){
                    bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                        should.not.exist(err)
                        should.not.exist(data)
                        done()
                    })
                })
            })
        })
        it("should return undefined if error found",function(done){
            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(data)
                done()
            })
        })

        it("should fail if the consumer group doesn't exist",function(done){
            redisClient1.set("topics:testTopic:head",0,function(err,data){
                redisClient2.set("topics:testTopic:head",0,function(err,data){
                    bqClient.getMessage("testTopic","testGroup-no-exist",undefined,function(err,data){
                        should.exist(err)
                        done()
                    })
               })
           })
        })
    })

    describe("ack",function(){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testGroup",function(err){
                    should.not.exist(err)
                    done()
                })
            })
        })
        it("should receive the recipientCallback ack the message",function(done){
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err){
                    bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                        var recipientCallback = data.recipientCallback
                        var recipientData = bqClient.decodeRecipientCallback(recipientCallback)
                        var client
                        if(recipientData.nodeId == "redis1"){
                            client = redisClient1
                        }else{
                            client = redisClient2
                        }
                        client.zrangebyscore("topics:testTopic:consumers:testGroup:processing","-inf","+inf",function(err,data){
                            should.not.exist(err)
                            should.exist(data)
                            data.should.have.length(1)
                            bqClient.ackMessage("testTopic","testGroup",recipientCallback,function(err){
                                client.zrangebyscore("topics:testTopic:consumers:testGroup:processing","-inf","+inf",function(err,data){
                                    should.not.exist(err)
                                    should.not.exist(err)
                                    should.exist(data)
                                    data.should.have.length(0)
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })
        it("should fail if the target node is down",function(done){
             bqClient.postMessage("testTopic",{msg:"testMessage"},function(err){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err){
                    bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                        var recipientCallback = data.recipientCallback
                        var recipientData = bqClient.decodeRecipientCallback(recipientCallback)
                        zk.a_set("/bq/clusters/test/nodes/"+recipientData.nodeId,JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"DOWN"}),-1,function(rc, err,stat){
                            setTimeout(function(){
                                bqClient.ackMessage("testTopic","testGroup",recipientData.id,function(err){
                                    should.exist(err)
                                    done()
                                })
                            },50)
                        })
                    })
                })
             })
        })
    })

    describe("fail",function(){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testGroup",function(err){
                    should.not.exist(err)
                    done()
                })
            })
        })

        it("should fail the message using the recipientCallback",function(done){
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err){
                    bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                        should.not.exist(err)
                        var recipientCallback = data.recipientCallback
                        var recipientData = bqClient.decodeRecipientCallback(recipientCallback)
                        var client
                        if(recipientData.nodeId == "redis1"){
                            client = redisClient1
                        }else{
                            client = redisClient2
                        }
                        client.lrange("topics:testTopic:consumers:testConsumer:fails",0,-1,function(err,data){
                            should.not.exist(err)
                            should.exist(data)
                            data.should.have.lengthOf(0)
                            bqClient.failMessage("testTopic","testGroup",recipientCallback,function(err){
                                should.not.exist(err)
                                client.lrange("topics:testTopic:consumers:testGroup:fails",0,-1,function(err,data){
                                    should.not.exist(err)
                                    should.exist(data)
                                    data.should.have.lengthOf(1)
                                    done()
                                }) 
                            })
                        })
                    })
                })
            })
        })
        it("should fail if the target node is down",function(done){
             bqClient.postMessage("testTopic",{msg:"testMessage"},function(err){
                bqClient.postMessage("testTopic",{msg:"testMessage"},function(err){
                    bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                        var recipientCallback = data.recipientCallback
                        var recipientData = bqClient.decodeRecipientCallback(recipientCallback)
                        zk.a_set("/bq/clusters/test/nodes/"+recipientData.nodeId,JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"DOWN"}),-1,function(rc, err,stat){
                            setTimeout(function(){
                                bqClient.failMessage("testTopic","testGroup",recipientData.id,function(err){
                                    should.exist(err)
                                    done()
                                })
                            },50)
                        })
                    })
                })
             })

        })
   })
   
   describe("#listTopics",function(done){
       it("should list all the topics created into zookeeper",function(done){
           bqClient.listTopics(function(data){
               should.exist(data)
               data.should.have.lengthOf(0)
               bqClient.createTopic("testTopic",function(err){
                   should.not.exist(err)
                   bqClient.listTopics(function(data){
                   should.exist(data)
                   data.should.have.lengthOf(1)
                   done()
                  })
              })
           })
       })
   })
   describe("#getConsumerGroups",function(done){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                done()
            })
        })

        it("should get the consumer group list for a topic",function(done){
            bqClient.getConsumerGroups("testTopic",function(err,data){
                should.not.exist(err)
                data.should.be.empty
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    should.not.exist(err)
                    bqClient.getConsumerGroups("testTopic",function(err,data){
                        should.not.exist(err)
                        data.should.include("testConsumer")
                        data.should.have.length(1)
                        done()
                    })
                })
            })
        })
        it("should fail if the topic doesn't exist",function(done){
            bqClient.getConsumerGroups("testTopic-noExist",function(err,data){
                should.exist(err)
                done()
            })
        })

   })
   
        
})
