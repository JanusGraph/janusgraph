#!/bin/bash

set -e
set -u

echo "Installing public package signing keys with apt-key"
for u in 'http://packages.elasticsearch.org/GPG-KEY-elasticsearch' \
         'http://debian.datastax.com/debian/repo_key' \
         'http://aureliuspkg.s3.amazonaws.com/keys/aurelius.asc' ; do
    echo "Downloading key from $u"
    wget -O - "$u" | apt-key add -
done
echo "Keys installed"

SOURCES_D=/etc/apt/sources.list.d
echo "Adding apt repository files to $SOURCES_D"
( set -x 
  echo 'deb http://aureliuspkg.s3.amazonaws.com/deb unstable main' > "$SOURCES_D"/aurelius.sources.list
  echo 'deb http://debian.datastax.com/community stable main' > "$SOURCES_D"/cassandra.sources.list
  echo 'deb http://packages.elasticsearch.org/elasticsearch/1.0/debian stable main' > "$SOURCES_D"/elasticsearch.sources.list )

echo "Installing Cassandra, ES, and JanusGraph with apt-get"
apt-get update
apt-get install cassandra=2.0.7 elasticsearch=1.0.3 janusgraph

# Reduce Cassandra and Gremlin Server/JanusGraph heapsizes
echo 'export MAX_HEAP_SIZE=512M
export HEAP_NEWSIZE=128M' > /etc/default/cassandra

echo 'export MAX_HEAP_SIZE=512M
export HEAP_NEWSIZE=128M' > /etc/default/janusgraph
