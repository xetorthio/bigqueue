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
    describe("Posting messages",function(){
        it("should can post a message to /topics/topicName/messages")
    })
    it("should get an error if topic doesn't exist")
    it("should can get messages from a consumer group")
    it("should can get the same message if this is posted to 2 consumer groups")
    it("should different messages if to members of the same consumer group do a 'get message'")
    it("should receive the same message if the visibility window is rached")
    it("should enable to do a DELETE of a message so this message shouldn't be received another time")
    it("if a message is set as fail this should be received another time into the same consumer group")
})
