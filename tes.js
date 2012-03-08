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

fs.readFile('lib/postMessage.lua','ascii',function(err,strFile){
    var json = JSON.stringify({"msg":"dasdas"})
    var count = 0;
    var HITS=1
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
            if(count>(HITS-1))
                 console.log('Total:'+(Date.now()-total))
        })
    }
    });
})

