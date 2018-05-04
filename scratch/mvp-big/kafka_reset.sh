#!/bin/sh

ZK="ip-172-31-82-223.ec2.internal:2181"
T1="aknn-demo-twitter-images-base64"
T2="aknn-demo-feature-vectors"

kafka-topics.sh --zookeeper $ZK --delete --topic $T1
kafka-topics.sh --zookeeper $ZK --delete --topic $T2
kafka-topics.sh --zookeeper $ZK --create --topic $T1 --replication-factor 2 --partitions 10
kafka-topics.sh --zookeeper $ZK --create --topic $T2 --replication-factor 1 --partitions 10
kafka-topics.sh --zookeeper $ZK --list

