#!/bin/bash

if [[ -z "$ADVERTISED_HOST_NAME" ]]; then
    export ADVERTISED_HOST_NAME='localhost'
fi

echo "advertised.host.name=$ADVERTISED_HOST_NAME" >> /opt/kafka_config/server.properties

echo "Starting zookeeper and kafka..."
/opt/kafka_2.10-0.8.2.1/bin/zookeeper-server-start.sh /opt/kafka_config/zookeeper.properties &
ZOOKEEPER_SERVER_PID=$!
/opt/kafka_2.10-0.8.2.1/bin/kafka-server-start.sh /opt/kafka_config/server.properties &
KAFKA_SERVER_PID=$!
wait $KAFKA_SERVER_PID

