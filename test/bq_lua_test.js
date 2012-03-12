var should = require('should'),
    redis = require('redis'),
    spawn = require('child_process').spawn,
    fs = require('fs');

describe("Redis lua scripts",function(){
    var postMessageScript;
    var getMessageScript;
    var createConsumerScript;
    var createTopicScript;
    var ackMessageScript; 
    var failMessageScript; 
    var redisClient;
    before(function(done){
        fs.readFile('lib/getMessage.lua','ascii',function(err,strFile){
            should.not.exist(err)
            getMessageScript = strFile
            fs.readFile('lib/postMessage.lua','ascii',function(err,strFile){
                should.not.exist(err)
                postMessageScript = strFile
                fs.readFile('lib/createConsumer.lua','ascii',function(err,strFile){
                    should.not.exist(err)
                    createConsumerScript = strFile
                    fs.readFile('lib/createTopic.lua','ascii',function(err,strFile){
                        should.not.exist(err)
                        createTopicScript = strFile
                        fs.readFile('lib/ack.lua','ascii',function(err,strFile){
                            should.not.exist(err)
                            ackMessageScript = strFile
                            fs.readFile('lib/fail.lua','ascii',function(err,strFile){
                                should.not.exist(err)
                                failMessageScript = strFile
                                redisClient = redis.createClient()
                                redisClient.on("ready",function(){
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })
    })

    describe("#createTopicScript",function(){
        beforeEach(function(done){
            redisClient.flushall(function(err){
                done();
            })
        })
        it("should add the topicKey to topics set",function(done){
            redisClient.eval(createTopicScript,0,"testTopic",function(err,data){
                should.not.exist(err)
                redisClient.sismember("topics","testTopic",function(err,data){
                    should.not.exist(err)
                    data.should.be.ok
                    done()
                })
            })
        })
        it("should set the property topics:topic:ttl default if no set as parameter",function(done){
            redisClient.eval(createTopicScript,0,"testTopic",function(err,data){
                redisClient.get("topics:testTopic:ttl",function(err,data){
                    should.not.exist(err)
                    should.exist(data)
                    done()
                })
            })
        })
        it("should set the property topics:topic:ttl send as parameter",function(done){
            redisClient.eval(createTopicScript,0,"testTopic","1",function(err,data){
                 redisClient.get("topics:testTopic:ttl",function(err,data){
                    should.not.exist(err)
                    should.exist(data)
                    data.should.be.equal("1")
                    done()
                })
            })
        })

        it("should fail if topic already exist",function(done){
            redisClient.eval(createTopicScript,0,"testTopic",function(err,data){
                should.not.exist(err)
                redisClient.eval(createTopicScript,0,"testTopic",function(err,data){
                    should.exist(err)
                    done()
                })
            })
        })
    })

    describe("#createConsumer",function(done){
        beforeEach(function(done){
            redisClient.flushall(function(err,data){
                should.not.exist(err)
                redisClient.eval(createTopicScript,0,"testTopic",function(err,data){
                    should.not.exist(err)
                    done()
                })
            })
        })

        it("should create the key topics:topic:consumers:consumer:last with value 1 if the head of topic dosn't exist",function(done){
            redisClient.eval(createConsumerScript,0,"testTopic","testConsumer",function(err){
                should.not.exist(err)
                redisClient.get("topics:testTopic:consumers:testConsumer:last",function(err,data){
                    should.not.exist(err)
                    should.exist(data)
                    data.should.be.equal("1")
                    done()
                })
            })

        })
        it("should create the key topics:topic:consumer:last with the head of the topic",function(done){
            var random=Math.floor(Math.random()*1000)
            redisClient.set("topics:testTopic:head",random,function(err,data){
                should.not.exist(err)
                redisClient.eval(createConsumerScript,0,"testTopic","testConsumer",function(err){
                    should.not.exist(err)
                    redisClient.get("topics:testTopic:consumers:testConsumer:last",function(err,data){
                        should.not.exist(err)
                        should.exist(data)
                        data.should.equal(""+random)
                        done()
                    })
                })
            })
        });
        it("should add the consumer to topics:topic:consumers set",function(done){
             redisClient.eval(createConsumerScript,0,"testTopic","testConsumer",function(err){
                 redisClient.sismember("topics:testTopic:consumers","testConsumer",function(err,data){
                     should.not.exist(err)
                     should.exist(data)
                     data.should.equal(1)
                     done()
                 })
             })
        })
        it("should fail if the topic doesn't exist",function(done){
             redisClient.eval(createConsumerScript,0,"testTopic-inexistent","testConsumer",function(err){
                 should.exist(err)
                 done()
             })

        })
        it("should fail if the consumer already exist",function(done){
             redisClient.eval(createConsumerScript,0,"testTopic","testConsumer",function(err){
                 should.not.exist(err)
                 redisClient.eval(createConsumerScript,0,"testTopic","testConsumer",function(err){
                     should.exist(err)
                     done()
                 })
             })
        })
    })

    describe("#postMessage",function(){
        var simpleMessage = JSON.stringify({msg:"testMessage"})
        beforeEach(function(done){
            redisClient.flushall(function(err,data){
                redisClient.eval(createTopicScript, 0, "testTopic", function(err,data){
                    should.not.exist(err)
                    redisClient.eval(createConsumerScript, 0, "testTopic", "testConsumer", function(err,data){
                        should.not.exist(err)
                        done()
                    })
                })
            })
        })

        it("should create the entry topics:topic:messages:msgId and return the id",function(done){
            redisClient.eval(postMessageScript,0,"testTopic",simpleMessage,function(err,data){
                should.not.exist(err)
                should.exist(data)
                var id = data
                redisClient.hgetall("topics:testTopic:messages:"+id,function(err,data){
                    should.not.exist(err)
                    should.exist(data)
                    data.should.have.keys("msg")
                    data.msg.should.equal("testMessage")
                    done()
                })
            })
        })
        it("should increment sequentially the property topics:topic:head",function(done){
            redisClient.eval(postMessageScript,0,"testTopic",simpleMessage,function(err,data){
                should.not.exist(err)
                var id1=data
                redisClient.eval(postMessageScript,0,"testTopic",simpleMessage,function(err,data){
                    should.not.exist(err)
                    var id2=data
                    redisClient.eval(postMessageScript,0,"testTopic",simpleMessage,function(err,data){
                        should.not.exist(err)
                        var id3=data
                        id2.should.equal(id1+1)
                        id3.should.equal(id2+1)
                        done()
                    })
                })
            })
        })
        it("the last id returned should be equals to the proprety topics:topic:head",function(done){
            redisClient.eval(postMessageScript,0,"testTopic",simpleMessage,function(err,data){
                var id = data;
                redisClient.get("topics:testTopic:head",function(err,data){
                    var eq = id == data
                    eq.should.be.ok
                    done()
                })
            })

        })

        it("should set exipire to the message",function(done){
            redisClient.eval(postMessageScript,0,"testTopic",simpleMessage,function(err,messageId){
                should.not.exist(err)
                    redisClient.ttl("topics:testTopic:messages:"+messageId,function(err,expire){
                     should.not.exist(err)
                     should.exist(expire)
                     redisClient.get("topics:testTopic:ttl",function(err,ttl){
                        should.not.exist(err)
                        should.exist(ttl)
                        expire.should.be.below(ttl+1)
                        expire.should.be.above(ttl-1)
                        done()
                    })
                })
            })
        })
        it("should fail if the property msg doesn't exist",function(done){
            redisClient.eval(postMessageScript,0,"testTopic",JSON.stringify({}),function(err,messageId){
                should.exist(err)
                done()
            })
        })
        it("should fail if topic doesn't exist",function(done){
            redisClient.eval(postMessageScript,0,"testTopic-noexist",simpleMessage,function(err,messageId){
                should.exist(err)
                done()
            })
        })
        it("should fail if no ttl found",function(done){
            redisClient.del("topics:testTopic:ttl",function(err,data){
                should.not.exist(err)
                redisClient.eval(postMessageScript,0,"testTopic",simpleMessage,function(err,messageId){
                    should.exist(err)
                    done()
                })
            })
        })
    })

    describe("#getMessage",function(){

        var tms = Math.floor(new Date().getTime()/1000)
        beforeEach(function(done){
            redisClient.flushall(function(err,data){
                redisClient.eval(createTopicScript, 0, "testTopic", function(err,data){
                    should.not.exist(err)
                    redisClient.eval(createConsumerScript, 0, "testTopic", "testConsumer", function(err,data){
                        should.not.exist(err)
                        redisClient.eval(postMessageScript, 0, "testTopic", JSON.stringify({msg:"testMessage"}), function(err, data){
                            should.not.exist(err)
                            done()
                        })
                    })
                })
            })
        })

        var redisListToObj = function(redisList){
            var o = {}
            for(var i=0;i<redisList.length;i=i+2){
                o[redisList[i]] = redisList[i+1]
            }
            return o
        }

        it("should get the message with id equals to topics:topic:consumers:consumer:last if no fails found", function(done){
            redisClient.get("topics:testTopic:consumers:testConsumer:last",function(err,data){
                var last = data;
                redisClient.eval(getMessageScript,0,tms,"testTopic","testConsumer","20",function(err,data){
                    should.not.exist(err)
                    var obj = redisListToObj(data)
                    obj.id.should.equal(last)
                    done()
                })
            })
        })
        it("should put the message id into topics:topic:consumers:consumer:processing with the expiration time equals to tms+visibilityWindow",function(done){
            redisClient.eval(getMessageScript,0,tms,"testTopic","testConsumer","20",function(err,data){
                should.not.exist(err)
                var obj = redisListToObj(data)
                redisClient.zrangebyscore("topics:testTopic:consumers:testConsumer:processing","-inf","+inf","WITHSCORES",function(err,data){
                    should.not.exist(err)
                    data[0].should.equal(""+1)
                    data[1].should.equal(""+(tms+20))
                    done()
                })
            })
        })
        it("should move to topics:topic:consumers:consumer:fails the expired message into topics:topic:consumers:consumer:processing",function(done){
            var expiredTime = tms -1
            var noExpiredTime = tms + 1
            redisClient.zadd("topics:testTopic:consumers:testConsumer:processing",noExpiredTime,"1",function(err,data){
                redisClient.zadd("topics:testTopic:consumers:testConsumer:processing",expiredTime,"2",function(err,data){
                    redisClient.zadd("topics:testTopic:consumers:testConsumer:processing",expiredTime,"3",function(err,data){
                        should.not.exist(err)
                        redisClient.eval(getMessageScript,0,tms,"testTopic","testConsumer","20",function(err,data){
                            redisClient.lrange("topics:testTopic:consumers:testConsumer:fails",0,-1,function(err,data){
                                data.should.have.length(2)
                                data.should.include("2")
                                data.should.include("3")
                                done()
                            })
                        })
                    })
                })
            })
        })
        it("should get a failed message over an stander message if topics:topic:consumers:consumer:fails is not empty",function(done){
            redisClient.eval(getMessageScript,0,tms,"testTopic","testConsumer","20",function(err,data){
                var expired = tms-21
                redisClient.eval(postMessageScript, 0, "testTopic", JSON.stringify({msg:"testMessage"}), function(err, newId){
                    should.not.exist(err)
                    redisClient.lpush("topics:testTopic:consumers:testConsumer:fails",newId,function(err,data){
                        redisClient.eval(getMessageScript,0,expired,"testTopic","testConsumer","20",function(err,data1){
                            should.not.exist(err)
                            redisClient.eval(getMessageScript,0,tms,"testTopic","testConsumer","20",function(err,data2){
                                var obj1 = redisListToObj(data1)
                                var obj2 = redisListToObj(data2)
                                should.exist(obj1)
                                should.exist(obj2)
                                obj1.id.should.equal(""+newId)
                                done()
                            })
                        })
                    })
                })
            })
        })
        it("should put into the processing list a failed message got")
        it("should throw an error if a failed message was expired")
        it("should increment the topics:topic:consumers:consumer:last after a get")
        it("shouldn't increment the topics:topic:consumers:consumer:last if the message is from the fails list")
        it("shouldn't no increment the topics:topic:consumers:consumer:last if there are no more messages")
        it("should return null if no object was found")
        it("should fail if topic doesn't exist")
        it("should fail if the consumerGrouo dosn't exist") 
    })
})
