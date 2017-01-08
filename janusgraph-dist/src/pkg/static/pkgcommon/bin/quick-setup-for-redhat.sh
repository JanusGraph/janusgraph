#!/bin/bash

set -e
set -u

echo "Installing public package signing keys with apt-key"
for u in 'http://packages.elasticsearch.org/GPG-KEY-elasticsearch' \
         'http://aureliuspkg.s3.amazonaws.com/keys/aurelius.asc' ; do
    echo "Downloading key from $u"
    rpm --import $u
done
echo "Keys installed"

SOURCES_D=/etc/yum.repos.d
echo "Adding yum repository files to $SOURCES_D"
cat > "$SOURCES_D"/aurelius.repo <<EOF
[aurelius]
name=Aurelius RPMs
baseurl=http://aureliuspkg.s3.amazonaws.com/rpm
enabled=1
gpgcheck=1
gpgkey=http://aureliuspkg.s3.amazonaws.com/keys/aurelius.asc
EOF
cat > "$SOURCES_D"/datastax.repo <<EOF
[datastax]
name=DataStax Repo for Apache Cassandra
baseurl=http://rpm.datastax.com/community
enabled=1
gpgcheck=0
EOF
cat > "$SOURCES_D"/elasticsearch.repo <<EOF
[elasticsearch-1.0]
name=Elasticsearch repository for 1.0.x packages
baseurl=http://packages.elasticsearch.org/elasticsearch/1.0/centos
enabled=1
gpgcheck=1
gpgkey=http://packages.elasticsearch.org/GPG-KEY-elasticsearch
EOF

echo "Installing Cassandra, ES, and JanusGraph with yum"
yum update
yum install cassandra20-2.0.7 elasticsearch-1.0.3 janusgraph

# Reduce Cassandra and Gremlin Server/JanusGraph heapsizes
echo 'export MAX_HEAP_SIZE=512M
export HEAP_NEWSIZE=128M' > /etc/default/cassandra

echo 'export MAX_HEAP_SIZE=512M
export HEAP_NEWSIZE=128M' > /etc/sysconfig/janusgraph
