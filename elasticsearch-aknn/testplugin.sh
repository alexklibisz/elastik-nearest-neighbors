#!/bin/sh
# Script for quickly recompiling and testing the elasticsearch-aknn plugin.
set -e

ESBIN="$HOME/Downloads/elasticsearch-6.2.4/bin"
PLUGINPATH="file:build/distributions/elasticsearch-aknn-0.0.1-SNAPSHOT.zip"

# TODO: fix the code so that skipping these tasks is not necessary.
gradle clean build -x integTestRunner -x checkStyleMain -x licenseHeaders -x test -x forbiddenApisMain
$ESBIN/elasticsearch-plugin remove elasticsearch-aknn
$ESBIN/elasticsearch-plugin install -b $PLUGINPATH
$ESBIN/elasticsearch


