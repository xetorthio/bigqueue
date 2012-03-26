#!/usr/local/bin/node

/**
 * Executable to create the http api client
 * If no config found a default config will be used, this confing
 * will use a redis localhost running at default port
 */
var bq = require('./lib/bq_client.js'),
    ZK = require('zookeeper'),
    bqc = require('./lib/bq_cluster_client.js'),
    http_api = require("./http_api.js")

var externalConfig = process.argv[2]

//Default redis conf
var redisLocalhost = {
    host:"127.0.0.1",
    port:6379
}

//Default api conf
var httpApiConfig = {
    "port": 8080,
    "bqConfig": redisLocalhost, 
    "bqClientCreateFunction": bq.createClient
}

//Check for external config
var config 
if(externalConfig){
    config = require(externalConfig).httpApiConfig
}else{
    config = httpConfig
}

//Run config
console.log("Using config: "+JSON.stringify(config))
http_api.startup(config)
