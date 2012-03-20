define REDIS1_CONF
daemonize yes
port 6379
pidfile /var/run/redis1.pid
endef

define REDIS2_CONF
daemonize yes
port 6380
pidfile /var/run/redis2.pid
endef

export REDIS1_CONF
export REDIS2_CONF

test:
	echo "$$REDIS1_CONF" | redis-server -
	echo "$$REDIS2_CONF" | redis-server -
	node_modules/zookeeper/build/zookeeper-3.4.3/bin/zkServer.sh start

	./node_modules/.bin/_mocha --globals myThis,myHolder,myCallee --reporter spec

	node_modules/zookeeper/build/zookeeper-3.4.3/bin/zkServer.sh stop

	kill `cat /var/run/redis1.pid`
	kill `cat /var/run/redis2.pid`

.PHONY: test
