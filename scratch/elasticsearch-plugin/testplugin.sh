#!/bin/sh

ESBIN="$HOME/Downloads/elasticsearch-6.2.4/bin"
PLUGINPATH="file:$HOME/dev/approximate-vector-search/scratch/elasticsearch-plugin/ingest-awesome/build/distributions/ingest-awesome-0.0.1-SNAPSHOT.zip"

cd ingest-awesome && gradle build -x integTestRunner -x checkStyleMain -x licenseHeaders
$ESBIN/elasticsearch-plugin remove ingest-awesome
$ESBIN/elasticsearch-plugin install -b $PLUGINPATH
$ESBIN/elasticsearch


