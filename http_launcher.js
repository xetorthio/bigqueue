var bq = require('./lib/bq_client.js'),
    ZK = require('zookeeper'),
    bqc = require('./lib/bq_cluster_client.js'),
    http_api = require("./http_api.js")

var clusterPath = "/bq/clusters/test"

var zkConfig = {
        connect: "localhost:2181",
        timeout: 200000,
        debug_level: ZK.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false
    }   

var bqClientConfig = {
    "zkConfig":zkConfig,
    "zkClusterPath":clusterPath,
    "createClientFunction":bq.createClient
}

var httpApiConfig = {
    "port": 8080,
    "bqConfig": bqClientConfig, 
    "bqClientCreateFunction": bqc.createClusterClient
}

http_api.startup(httpApiConfig)
