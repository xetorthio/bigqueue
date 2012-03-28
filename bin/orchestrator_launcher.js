#!/usr/local/bin/node

var ZK = require("zookeeper"),
    oc = require("../lib/bq_cluster_orchestrator.js"),
    bq = require("../lib/bq_client.js")

var externalConfig = process.argv[2]

var zkConfig = {
        connect: "localhost:2181",
        timeout: 200000,
        debug_level: ZK.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false
    }   

var ocConfig = {
    "zkClustersPath":"/bq/clusters",
    "zkConfig":zkConfig,
    "createNodeClientFunction":bq.createClient,
    "checkInterval":2000
}

var config 
if(externalConfig){
    config = require(externalConfig).orchestratorConfig
}else{
    config = ocConfig 
}

var orch = oc.createOrchestrator(config)
orch.on("ready",function(){
    console.log("Orchestrator running")
})

