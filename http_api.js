var app = require('express').createServer()
var bqClient

app.post("/topics",function(req,res){
    req.on("data",function(body){
        var topic = JSON.parse(body)
        bqClient.createTopic(topic.name,function(err){
            if(err){
                res.json(err,409)
            }else{
                res.json({name:topic.name},201)
            }
        })
    })
})

app.post("/topics/:topic/consumerGroups",function(req,res){
    req.on("data",function(body){
        var consumer = JSON.parse(body)
        var topic = req.params.topic
        bqClient.createConsumerGroup(topic,consumer.name,function(err){
            if(err){
                res.json(err,409)
            }else{
                res.json({name:consumer.name},201)
            }
        })
    })
})

exports.startup = function(config){
    app.listen(config.port)
    bqClient = config.bqClientCreateFunction(config.bqConfig)
    console.log("http api running on ["+config.port+"]")
}

exports.shutdown = function(){
}
