define REDIS1_CONF
daemonize yes
port 6379
pidfile /tmp/redis1.pid
endef

define REDIS2_CONF
daemonize yes
port 6380
pidfile /tmp/redis2.pid
endef

export REDIS1_CONF
export REDIS2_CONF
export ZOOCFG := test/resources/zoo.cfg
test:
	echo "$$REDIS1_CONF" | redis-server -
	echo "$$REDIS2_CONF" | redis-server -
	

	node_modules/zookeeper/build/zookeeper-3.4.3/bin/zkServer.sh start ${ZOOCFG}

	./node_modules/.bin/_mocha --globals myThis,myHolder,myCallee,State_myThis --reporter spec -t 5000 -s 3000 ${TESTFILE}

	node_modules/zookeeper/build/zookeeper-3.4.3/bin/zkServer.sh stop ${ZOOCFG}

	kill `cat /tmp/redis1.pid`
	kill `cat /tmp/redis1.pid`

.PHONY: test
