#!/bin/sh

SERVER=$1
TOPIC=$2

echo $SERVER
echo $TOPIC

kafka-console-consumer.sh --bootstrap-server "$SERVER" --topic "$TOPIC" \
    --from-beginning \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property print.key=true \
    --property print.value=false
