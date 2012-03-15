var should = require('should'),
    redis = require('redis')
    bq = require('../lib/bq_client.js')

describe("Big Queue Client",function(){
    
    var redisClient
    var redisConf= {host:"127.0.0.1",port:6379}
    var bqClient
    before(function(done){
        bqClient = bq.createClient(redisConf)
        bqClient.on("ready",function(){
            redisClient = redis.createClient()
            redisClient.on("ready",function(){
                done()
            })
        })
    })    

    beforeEach(function(done){
        redisClient.flushall(function(data,err){
            done()
        })
    })    

    describe("#createTopic",function(){
        it("should add the topic to the topic list",function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                redisClient.sismember("topics","testTopic",function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    done()
                })
            })
        })
        it("should set a ttl if is set as parameter",function(done){
            bqClient.createTopic("testTopic",1,function(err){
                should.not.exist(err)
                redisClient.get("topics:testTopic:ttl",function(err,data){
                    should.not.exist(err)
                    data.should.equal(""+1)
                    done()
                })
            })
        })
        it("should get an error if the topic already exist",function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                bqClient.createTopic("testTopic",function(err){
                    should.exist(err)
                    done()
                })
            })
        })
    }) 

    describe("#createConsumerGroup",function(){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                done()
            })
        })
        it("should add the consumer to the consumer list",function(done){
            bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                should.not.exist(err)
                redisClient.sismember("topics:testTopic:consumers","testConsumer",function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    done()
                })
            })
        })
        it("should add the consumer key 'last'",function(done){
            bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                should.not.exist(err)
                redisClient.exists("topics:testTopic:consumers:testConsumer:last",function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    done()
                })
            })
        })
        it("should get an error if the consumer group already exist",function(done){
            bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
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

        it("should add the message to redis and return their key",function(done){
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key){
                should.not.exist(err)
                key.id.should.be.above(0)
                redisClient.exists("topics:testTopic:messages:"+key.id,function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    done()
                })
            })
        })
        it("should return an error if topic doesn't exist",function(done){
            bqClient.postMessage("testTopic-noExist",{msg:"testMessage"},function(err,key){
                should.exist(err)
                done()
            })
        })
    })

    describe("#getMessage",function(){
         var generatedKey
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                        generatedKey = key.id
                        should.not.exist(err)
                        done()
                    })
                })
            })
        })

        it("should get a message stored on redis",function(done){
            bqClient.getMessage("testTopic","testConsumer",function(err,data){
                should.not.exist(err)
                data.should.have.keys("msg","id")
                data.id.should.equal(""+generatedKey)
                done()
            })    
        })
        it("should override visibility window if one is set as param",function(done){
            var tmsExpired = Math.floor(new Date().getTime()/1000)+10
            bqClient.getMessage("testTopic","testConsumer",10,function(err,data){
                redisClient.zrangebyscore("topics:testTopic:consumers:testConsumer:processing","-inf","+inf","withscores",function(err,data){
                    should.not.exist(err)
                    data[1].should.be.above(tmsExpired-2)
                    data[1].should.be.below(tmsExpired+1)
                    done()
                })

            })
        })
        it("should get an error if consumer group doesn't exist",function(done){
            bqClient.getMessage("testTopic","testConsumer-noExist",function(err,data){
                should.exist(err)
                done()
            })
        })
    })

    describe("#ackMessage",function(){
         var id;
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                        bqClient.getMessage("testTopic","testConsumer",function(err,msg){
                            should.not.exist(err)
                            should.exist(msg)
                            id = msg.id
                            done()
                        })
                    })
                })
            })
        })

        it("should set a message as processed",function(done){
            bqClient.ackMessage("testTopic","testConsumer",id,function(err){
                should.not.exist(err)
                redisClient.zrangebyscore("topics:testTopic:consumers:testConsumer:processing","-inf","+inf",function(err,data){
                    should.not.exist(err)
                    data.should.not.include(""+id)
                    done()
                })
            })    
        })
        it("should get an error if message can't be acked",function(done){
            bqClient.ackMessage("testTopic","testConsumer",15,function(err){
                should.exist(err)
                done()
            })
        })
        it("should get an error if consumer group doesn't exist",function(done){
            bqClient.ackMessage("testTopic","testConsumer-noExist",15,function(err){
                should.exist(err)
                done()
            })
        })
    })

    describe("#failMessage",function(){
         var id;
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                        bqClient.getMessage("testTopic","testConsumer",function(err,msg){
                            should.not.exist(err)
                            should.exist(msg)
                            id = msg.id
                            done()
                        })
                    })
                })
            })
        })

        it("should move a message to the fails list",function(done){
            bqClient.failMessage("testTopic","testConsumer",id,function(err){
                should.not.exist(err)
                redisClient.lrange("topics:testTopic:consumers:testConsumer:fails",0,-1,function(err,data){
                    should.not.exist(err)
                    data.should.include(id)
                    done()
                })
            }) 
        })
        it("should get an error if consumer group dosn't exist",function(done){
            bqClient.failMessage("testTopic","testConsumer-noExist",id,function(err){
                should.exist(err)
                done()
            })
        })
    })

    describe("#listTopics",function(){
        it("should get the topic list",function(done){
            bqClient.listTopics(function(data){
                data.should.be.empty
                bqClient.createTopic("testTopic",function(err){
                    should.not.exist(err)
                    bqClient.listTopics(function(data){
                        data.should.include("testTopic")
                        done()
                    })
                })
            })
        })
    })

    describe("#listConsumerGroups",function(){
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

    describe("#getTopicsStats",function(){
        it("should return the las message id")
    })    

    describe("#getConsumerGroupStats",function(){
        it("should get the amount of processing messages")
        it("should get the amount of failed messages")
        it("should get the amount of unprocess messages")
        it("should fail if consumer group doesn't exist")
    })
    

})

