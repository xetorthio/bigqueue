var express = require('express')
var app = express.createServer()
var maxBody = 64*1024
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
    var data=""
    req.on("data",function(body){
        data=data+body.toString()
    })
    req.on("end",function(){
        try{
            var consumer = JSON.parse(data)
        }catch(e){
            res.json({err:e},400)
            return
        }
        var topic = req.params.topic
        bqClient.createConsumerGroup(topic,consumer.name,function(err){
            console.log(err)
            if(err){
                res.json(err,409)
            }else{
                res.json({name:consumer.name},201)
            }
        })

    })
})

app.post("/topics/:topic/messages",function(req,res){
    var excedes = false 
    var data = ""
    req.on("data",function(body){
        data = data+body.toString()
        if(data.length > maxBody){
            res.json({err:"Body too long"},414)
            excedes = true
            return
        }
    })
    req.on("end",function(){
        if(!excedes){
            var message = JSON.parse(data)
            bqClient.postMessage(req.params.topic,message,function(err,data){
                if(err){
                    res.json(err,400)
                }else{
                    res.json(data,201)
                }
            })
        }
    })
})

app.get("/topics/:topic/consumerGroups/:consumer/messages",function(req,res){
    bqClient.getMessage(req.params.topic,req.params.consumer,req.query.visibilityWindow,function(err,data){
        if(err){
            res.json(err,400)
        }else{
            if(data && data.id)
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
    app.use(express.logger({ format: ':method :url' }))
    app.listen(config.port)
    bqClient = config.bqClientCreateFunction(config.bqConfig)
    console.log("http api running on ["+config.port+"]")
}

exports.shutdown = function(){
}
