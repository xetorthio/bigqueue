var should = require('should'),
    bq = require('../lib/bq_client.js'),
    httpApi = require("../http_api.js")
    redis = require('redis'),
    request = require('request')

describe("http api",function(){
    var redisClient
    var http_api
    var redisConf= {host:"127.0.0.1",port:6379}

    var httpApiConf = {
        "port": 8080,
        "bqConfig": redisConf, 
        "bqClientCreateFunction": bq.createClient
    }

    before(function(done){
        redisClient = redis.createClient()
        redisClient.on("ready",function(){
            httpApi.startup(httpApiConf,function(err){
                done()
            })
            done()
        })
    })

    beforeEach(function(done){
        redisClient.flushall(function(err,data){
            done()
        })
    })

    after(function(done){
        httpApi.shutdown()
        done()
    })

    describe("Create Topics",function(){
        it("should enable to create topics and return the created topic name",function(done){
            request({
                url:"http://127.0.0.1:8080/topics",
                method:"POST",
                json:{name:"testTopic"}
            },function(error,response,body){
                should.not.exist(error)
                should.exist(response)
                response.statusCode.should.equal(201)
                body.should.have.property("name")
                done()
            })
        })
        it("should get an error topic already exist",function(done){
            request({
                url:"http://127.0.0.1:8080/topics",
                method:"POST",
                json:{name:"testTopic"}
            },function(error,response,body){
                should.not.exist(error)
                should.exist(response)
                response.statusCode.should.equal(201)
                body.should.have.property("name")
                request({
                   url:"http://127.0.0.1:8080/topics",
                   method:"POST",
                   json:{name:"testTopic"}
                },function(error,response,body){
                    response.statusCode.should.equal(409)
                    body.should.have.property("err")
                    done()
                })

            })
        })
    })
    
    describe("Create Consumer Groups",function(){
        beforeEach(function(done){
            request({
                url:"http://127.0.0.1:8080/topics",
                method:"POST",
                json:{name:"testTopic"}
            },function(error,response,body){
                done()
            }) 
        })
        it("should enable to create consumer groups and return the create group",function(done){
            request({
                url:"http://127.0.0.1:8080/topics/testTopic/consumerGroups",
                method:"POST",
                json:{name:"testConsumer"}
            },function(error,response,body){
                should.not.exist(error)
                should.exist(response)
                response.statusCode.should.equal(201)
                body.should.have.property("name")
                done()
            })
        })

        it("should get an error if consumer group already exist",function(done){
            request({
                url:"http://127.0.0.1:8080/topics/testTopic/consumerGroups",
                method:"POST",
                json:{name:"testConsumer"}
            },function(error,response,body){
                should.not.exist(error)
                should.exist(response)
                request({
                    url:"http://127.0.0.1:8080/topics/testTopic/consumerGroups",
                    method:"POST",
                    json:{name:"testConsumer"}
                },function(error,response,body){
                    should.not.exist(error)
                    should.exist(response)
                    response.statusCode.should.equal(409)
                    body.should.have.property("err")
                    done()
                })
            })

        })
        it("should get an error if topic doesn't exist",function(done){
            request({
                url:"http://127.0.0.1:8080/topics/testTopic-no-exist/consumerGroups",
                method:"POST",
                json:{name:"testConsumer"}
            },function(error,response,body){
                should.not.exist(error)
                should.exist(response)
                response.statusCode.should.equal(409)
                done()
            })
        })
    })
    describe("Exchange Messages",function(done){
        beforeEach(function(done){
            request({
                url:"http://127.0.0.1:8080/topics",
                method:"POST",
                json:{name:"testTopic"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/topics/testTopic/consumerGroups",
                    method:"POST",
                    json:{name:"testConsumer1"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/topics/testTopic/consumerGroups",
                        method:"POST",
                        json:{name:"testConsumer2"}

                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        done()
                    })
                })
            }) 

        })
        it("should receive posted messages",function(done){
            request({
                uri:"http://127.0.0.1:8080/topics/testTopic/messages",
                method:"POST",
                json:{msg:"testMessage"}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body.should.have.property("id")
                var postId = body.id
                request({
                    uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    done()
                })
            })
        })
        it("should return 204 http status if no data could be getted",function(done){
            request({
                uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(204)
                done()
            })
        })
        it("should get an error if we try to get messages from non existent topic",function(done){
            request({
                uri:"http://127.0.0.1:8080/topics/testTopic-dsadsa/consumerGroups/testConsumer1/messages",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(400)
                done()
            })
        })
        it("should can get the same message if there are 2 consumer groups",function(done){
            request({
                uri:"http://127.0.0.1:8080/topics/testTopic/messages",
                method:"POST",
                json:{msg:"testMessage"}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body.should.have.property("id")
                var postId = body.id
                request({
                    uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    request({
                        uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer2/messages",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        should.exist(response)
                        response.statusCode.should.equal(200)
                        body.should.have.property("id")
                        body.should.have.property("msg")
                        body.id.should.equal(""+postId)
                        body.msg.should.equal("testMessage")
                        done() 
                    })
                })
            })
        })
        it("should get different messages if 2 members of the same consumer group do a 'get message'",function(done){
            request({
                uri:"http://127.0.0.1:8080/topics/testTopic/messages",
                method:"POST",
                json:{msg:"testMessage"}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body.should.have.property("id")
                var postId1 = body.id
                request({
                    uri:"http://127.0.0.1:8080/topics/testTopic/messages",
                    method:"POST",
                    json:{msg:"testMessage"}
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(201)
                    body.should.have.property("id")
                    var postId2 = body.id
                    request({
                        uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        should.exist(response)
                        response.statusCode.should.equal(200)
                        body.should.have.property("id")
                        body.should.have.property("msg")
                        body.id.should.equal(""+postId1)
                        body.msg.should.equal("testMessage")
                        request({
                            uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            should.exist(response)
                            response.statusCode.should.equal(200)
                            body.should.have.property("id")
                            body.should.have.property("msg")
                            body.id.should.equal(""+postId2)
                            body.msg.should.equal("testMessage")
                            done()
                        })
                    })
                })
            })
        })
        it("should receive the same message if the visibility window is rached",function(done){
            request({
                uri:"http://127.0.0.1:8080/topics/testTopic/messages",
                method:"POST",
                json:{msg:"testMessage"}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body.should.have.property("id")
                var postId = body.id
                request({
                    uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages?visibilityWindow=1",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.should.have.property("recipientCallback")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    setTimeout(function(){
                        request({
                            uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages?visibilityWindow=1",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            should.exist(response)
                            response.statusCode.should.equal(200)
                            body.should.have.property("id")
                            body.should.have.property("msg")
                            body.should.have.property("recipientCallback")
                            body.id.should.equal(""+postId)
                            body.msg.should.equal("testMessage")
                            done()
                        })
                    },1100)
                })
            })
        })
        it("should enable to do a DELETE of a message so this message shouldn't be received another time",function(done){
            request({
                uri:"http://127.0.0.1:8080/topics/testTopic/messages",
                method:"POST",
                json:{msg:"testMessage"}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body.should.have.property("id")
                var postId = body.id
                request({
                    uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages?visibilityWindow=1",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    request({
                        uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages/"+body.recipientCallback,
                        method:"DELETE",
                        json:true
                    },function(err,response,body){
                        response.statusCode.should.equal(204)
                        setTimeout(function(){
                            request({
                                uri:"http://127.0.0.1:8080/topics/testTopic/consumerGroups/testConsumer1/messages?visibilityWindow=1",
                                method:"GET",
                                json:true
                            },function(error,response,body){
                                should.exist(response)
                                response.statusCode.should.equal(204)
                                done()
                            })
                        },1100)
                    })
                })
            })
        })
    })

    describe("List Elements",function(done){
        it("Should list all topics if a get is execute over /topics",function(done){
            request({
                uri:"http://127.0.0.1:8080/topics",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.should.be.empty
                request({
                    url:"http://127.0.0.1:8080/topics",
                    method:"POST",
                    json:{name:"testTopic"}
                },function(error,response,body){
                    request({
                        uri:"http://127.0.0.1:8080/topics",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.should.have.lengthOf(1)
                        body.should.include("testTopic")
                        done()
                    })
                })
            })
        })
        it("Should list all consumer groups if a get is execute over /topics/topic/consumerGroups",function(done){
            request({
                url:"http://127.0.0.1:8080/topics",
                method:"POST",
                json:{name:"testTopic"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/topics/testTopic/consumerGroups",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.be.empty
                    request({
                        url:"http://127.0.0.1:8080/topics/testTopic/consumerGroups",
                        method:"POST",
                        json:{name:"testConsumer"}
                    },function(error,response,body){
                        request({
                            url:"http://127.0.0.1:8080/topics/testTopic/consumerGroups",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.lengthOf(1)
                            body.should.include("testConsumer")
                            done()
                        })
                    })
                }) 
            })  
        })
    })

    describe("Limits",function(){
        it("should get an error if a posted message have more than 64kb",function(done){
            var b64kb = function(){
                var str = ""
                for(var i=0;i<(64*1024)+1;i++)
                    str=str+"a"
                return str
            }
            var msg = b64kb()
            request({
                uri:"http://127.0.0.1:8080/topics/testTopic/messages",
                method:"POST",
                body:msg,
                headers:{"content-length":msg.length}
            },function(err,response,body){
                should.not.exist(err)
                response.statusCode.should.equal(414)
                done()
            })
        })
    })
})
