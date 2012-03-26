/**
 * This example shows how to create a simple configuration to
 * connect the http api to a bigqueue cluster
 */

/*
 * Into a project where bigqueue is installed as `npm install bigqueue`
 * this line should be replaced by var bq = require("BigQueue")
 */
var bq = require("../lib/index.js"),
    ZK = require('zookeeper'),
    bqredis = bq.redisclient,
    bqc = bq.clusterclient

// Path to cluster data into zookeeper
var clusterPath = "/bq/clusters/test"

//Zookeeper configuration
var zkConfig = {
        connect: "localhost:2181",
        timeout: 200000,
        debug_level: ZK.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false
    }

//Cluster configuration
var bqClientConfig = {
    "zkConfig":zkConfig,
    "zkClusterPath":clusterPath,
    "createNodeClientFunction":bqredis.createClient
}

//Http api configuration
var httpApiConfig = {
    "port": 8080,
    "bqConfig": bqClientConfig,
    "bqClientCreateFunction": bqc.createClusterClient
}

//Export that will be used into the http_launcher to load the config
exports.httpApiConfig = httpApiConfig 
