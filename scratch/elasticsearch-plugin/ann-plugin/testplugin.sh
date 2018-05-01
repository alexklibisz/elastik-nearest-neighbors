#!/bin/sh

ESBIN="$HOME/Downloads/elasticsearch-6.2.4/bin"
PLUGINPATH="file:$HOME/dev/approximate-vector-search/scratch/elasticsearch-plugin/ann-plugin/build/distributions/ann-plugin-0.0.1-SNAPSHOT.zip"

gradle build -x integTestRunner -x checkStyleMain -x licenseHeaders -x test -x forbiddenApisMain
$ESBIN/elasticsearch-plugin remove ann-plugin
$ESBIN/elasticsearch-plugin install -b $PLUGINPATH
$ESBIN/elasticsearch


