#!/bin/bash

set -x

until $(curl --output /dev/null --silent --head --fail http://solr:8983); do sleep 5; done
for core in $(cat /tmp/collections.txt); do /opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost zookeeper:2181 -cmd upconfig -confdir mydata -confname $core; done

echo "All collections imported"
