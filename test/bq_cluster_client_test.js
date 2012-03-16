var should = require('should'),
    redis = require('redis'),
    ZK = require('zookeeper'),
    bq = require('../lib/bq_client.js'),
    bqc = require('../lib/bq_cluster_client.js')

describe("Big Queue Cluster",function(){
    
    //Prepare stage
    var clusterPath = "/bq/clusters/test"

    var zkConfig = {
            connect: "localhost:2181",
            timeout: 200000,
            debug_level: ZK.ZOO_LOG_LEVEL_WARN,
            host_order_deterministic: false,
            data_as_buffer:false
        }   

    var bqClientConfig = {
        "zkConfig":zkConfig,
        "zkClusterPath":clusterPath,
        "createClientFunction":bq.createClient
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

    describe("#getMessage",function(){
        it("should generate and add a recipientCallback to the returned message")
        it("should balance the gets throw all nodes")
        it("should transform the message id at get action for a uid for the cluster (the same uid that it've generated at post)")
        it("should return void array if no message found")
        it("should return void if no redis found")
        it("should fail if the consumer group doesn't exist")
    })
    
    describe("operations",function(){
        it("should notify error if a connection error found")
        it("should add to the node list new nodes found")
        it("should remove a node from node list if a node is put as DOWN")
        it("should remove a node from node list if a node is deleted")
        it("should mantains the topics and consumer list")
    })
       
    describe("#postMessage",function(){
        it("should try to resend the message to another node if an error ocurrs sending")
        it("should balance the writes throw all nodes")
        it("should transform the post id to a uid for the cluster")
        it("should notify an error to zookeeper on redis connection error")
        it("should cache the data and resend it if a node goes down")
        it("should try with other node if one fails")
    })

})
