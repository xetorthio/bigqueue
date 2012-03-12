/*var a ={
    host:"test",
    topic:"lala",
    key:"dasdas"
}
var j =JSON.parse(new Buffer("eyJob3N0IjoidGVzdCIsInRvcGljIjoibGFsYSIsImtleSI6ImRhc2RhcyJ9",'base64').toString())
console.log(j.host)
console.log(new Buffer(JSON.stringify(a)).toString('base64'))
*/
var redis=require('redis'),
    fs = require('fs')
var client = redis.createClient()
/**
fs.readFile('postMessage.lua','ascii',function(err,strFile){
    var json = JSON.stringify({"msg":"dasdas",creationTime:Date.now()})
    var count = 0;
    var HITS=100000
   client.on('ready',function(){
     var total= Date.now()
     console.log(total)
 for(var i=0; i< HITS;i++){
        var t1=Date.now()
//        client.evalsha("6c6e4a7c75bf85032184e25af4dc46750bff79d5",0,"test",json,function(err,data){
        client.eval(strFile,0,"test",json,function(err,data){
            count++;
            if(err)
                console.log(err)
            if(data)
                console.log(data)

            if(count>(HITS-1))
                 console.log('Total:'+(Date.now()-total))
        })
    }
    });
})
**/
fs.readFile('getMessages.lua','ascii',function(err,strFile){
    var count = 0;
    var HITS=100000
   client.on('ready',function(){
     var total= Date.now()
     console.log(total)
     for(var i=0; i< HITS;i++){
        var t1=Date.now()
        var tms = Math.floor(new Date().getTime()/1000)
        client.eval(strFile,0,tms,"test","consumerTest",function(err,data){
            count++;
            if(err)
                console.log(err)
            if(data){
                var obj = {}
                for(var i=0; i<data.length; i=i+2){
                    obj[data[i]]=data[i+1]
                }
               // console.log(obj)
            }

            if(count>(HITS-1))
                 console.log('Total:'+(Date.now()-total))
        })
    }
    });
})
