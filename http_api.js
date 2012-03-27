var express = require('express')
var app = express.createServer()
var maxBody = 64*1024
var bqClient

var valid_element_regex=/^(\w|[0-9]){2,50}$/
app.get("/topics",function(req,res){
    try{
        bqClient.listTopics(function(data){
            res.json(data,200)
        })
    }catch(e){
        res.json({err:"Error processing request ["+e+"]"},500)
    }

})

app.post("/topics",function(req,res){
   var data=""
   req.on("data",function(body){
       data=data+body.toString()
   })
   req.on("end",function(){
        var topic
        try{
            topic = JSON.parse(data)
        }catch(e){
            res.json({err:"Error parsing json ["+e+"]"},400)
            return
        }
        try{
            if(!topic.name.match(valid_element_regex)){
                res.json({err:"Topic should be an string without special chars between 2 and 50 chars"},400)
                return
            }
            bqClient.createTopic(topic.name,function(err){
                if(err){
                    res.json(err,409)
                }else{
                    res.json({name:topic.name},201)
                }
            })
        }catch(e){
            res.json({err:"Error processing request ["+e+"]"},500)
        }
    })
})

app.get("/topics/:topic/consumerGroups",function(req,res){
    try{
        bqClient.getConsumerGroups(req.params.topic,function(err,data){
   
            if(err){
                res.json(err,400)
            }else{
                res.json(data,200)
            }
        })
    }catch(e){
        res.json({err:"Error processing request ["+e+"]"},500)
    }

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
        if(!consumer.name.match(valid_element_regex)){
          res.json({err:"Consumer group should be an string without special chars between 2 and 50 chars"})
          return
        }

        try{
            bqClient.createConsumerGroup(topic,consumer.name,function(err){
                if(err){
                    res.json(err,409)
                }else{
                    res.json({name:consumer.name},201)
                }
            })
        }catch(e){
            res.json({err:"Error processing request ["+e+"]"},500)
        }


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
            var message
            try{
                message = JSON.parse(data)
 
            }catch(e){
                res.json({err:"Error parsing json ["+e+"]"},400)
                return
            }
            try{
                bqClient.postMessage(req.params.topic,message,function(err,data){
                    if(err){
                        res.json(err,400)
                    }else{
                        res.json(data,201)
                    }
                })
            }catch(e){
                res.json({err:"Error processing request ["+e+"]"},500)
            }

        }
    })
})

app.get("/topics/:topic/consumerGroups/:consumer/messages",function(req,res){
    try{
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
    }catch(e){
        res.json({err:"Error processing request ["+e+"]"},500)
    }
})

app.delete("/topics/:topic/consumerGroups/:consumer/messages/:recipientCallback",function(req,res){
    try{
        bqClient.ackMessage(req.params.topic,req.params.consumer,req.params.recipientCallback,function(err){
            if(err){
                res.json(err,404)
            }else{
                res.json({},204)
            }
        })
    }catch(e){
        res.json({err:"Error processing request ["+e+"]"},500)
    }

})

exports.startup = function(config){
    app.use(express.logger({ format: ':method :url' }))
    app.listen(config.port)
    bqClient = config.bqClientCreateFunction(config.bqConfig)
    console.log("http api running on ["+config.port+"]")
}

exports.shutdown = function(){
}
