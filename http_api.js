var app = require('express').createServer()
var bqClient

app.get("/topics",function(req,res){
    bqClient.listTopics(function(data){
        res.json(data,200)
    })
})

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

app.get("/topics/:topic/consumerGroups",function(req,res){
    bqClient.getConsumerGroups(req.params.topic,function(err,data){
        if(err){
            res.json(err,400)
        }else{
            res.json(data,200)
        }
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

app.post("/topics/:topic/messages",function(req,res){
    req.on("data",function(body){
        var message = JSON.parse(body)
        bqClient.postMessage(req.params.topic,message,function(err,data){
            if(err){
                res.json(err,400)
            }else{
                res.json(data,201)
            }
        })
    })
})

app.get("/topics/:topic/consumerGroups/:consumer/messages",function(req,res){
    bqClient.getMessage(req.params.topic,req.params.consumer,req.query.visibilityWindow,function(err,data){
        if(err){
            res.json(err,400)
        }else{
            if(data.id)
                res.json(data,200)
            else
                res.json({},204)
        }
    })
})

app.delete("/topics/:topic/consumerGroups/:consumer/messages/:recipientCallback",function(req,res){
    bqClient.ackMessage(req.params.topic,req.params.consumer,req.params.recipientCallback,function(err){
        if(err){
            res.json(err,404)
        }else{
            res.json({},204)
        }
    })
})

exports.startup = function(config){
    app.listen(config.port)
    bqClient = config.bqClientCreateFunction(config.bqConfig)
    console.log("http api running on ["+config.port+"]")
}

exports.shutdown = function(){
}
