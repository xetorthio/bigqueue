var should = require('should'),
    redis = require('redis')
    bqClient = require('../lib/bq_client.js')

describe("Big Queue Client",function(){
    
    var redisClient
    before(function(done){
        redisClient = redis.createClient()
        redisClient.on("ready",function(){
            done()
        })
    })    
    
    describe("#createTopic",function(){
        it("should add the topic to the topic list")
        it("should get an error if the topic already exist")
    }) 

    describe("#createConsumerGroup",function(){
        it("should add the consumer to the consumer list")
        it("should generate an 'id' as consumer name")
        it("should add the consumer key 'last'")
        it("should get an error if the consumer group already exist")
    })

    describe("#postMessage",function(){
        it("should get a message uid as result")
        it("should add the message key to redis")
        it("should return an error if topic doesn't exist")
    })

    describe("#getMessage",function(){
        it("should get a message stored on redis")
        it("should regenerate the id got as result of post action")
        it("should get an error if consumer group doesn't exist")
    })

    describe("#ackMessage",function(){
        it("should set a message as processed")
        it("should get an error if message can't be acked")
        it("should get an error if consumer group dosn't exist")
    })

    describe("#failMessage",function(){
        it("should move a message to the fails list")
        it("should get an error if consumer group dosn't exist")
    })

    describe("#listTopics",function(){
        it("should get the topic list")
    })

    describe("#listConsumerGroups",function(){
        it("should get the consumer group list for a topic")
        it("should fail if the topic doesn't exist")
    })

    describe("#getConsumerGroupStats",function(){
        it("should get the amount of processing messages")
        it("should get the amount of failed messages")
        it("should get the amount of unprocess messages")
        it("should fail if consumer group doesn't exist")
    })
    
    describe("#errors",function(){
        it("should call error event on any redis connection error")
    })

})

