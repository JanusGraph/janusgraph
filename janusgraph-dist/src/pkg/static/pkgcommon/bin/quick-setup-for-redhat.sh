#!/bin/bash
# Copyright 2019 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

set -e
set -u

echo "Installing public package signing keys with apt-key"
for u in 'https://packages.elasticsearch.org/GPG-KEY-elasticsearch' \
         'https://aureliuspkg.s3.amazonaws.com/keys/aurelius.asc' ; do
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
gpgkey=https://aureliuspkg.s3.amazonaws.com/keys/aurelius.asc
EOF
cat > "$SOURCES_D"/datastax.repo <<EOF
[datastax]
name=DataStax Repo for Apache Cassandra
baseurl=https://rpm.datastax.com/community
enabled=1
gpgcheck=0
EOF
cat > "$SOURCES_D"/elasticsearch.repo <<EOF
[elasticsearch-1.0]
name=Elasticsearch repository for 1.0.x packages
baseurl=http://packages.elasticsearch.org/elasticsearch/1.0/centos
enabled=1
gpgcheck=1
gpgkey=https://packages.elasticsearch.org/GPG-KEY-elasticsearch
EOF

echo "Installing Cassandra, ES, and JanusGraph with yum"
yum update
yum install cassandra20-2.0.7 elasticsearch-1.0.3 janusgraph

# Reduce Cassandra and Gremlin Server/JanusGraph heapsizes
echo 'export MAX_HEAP_SIZE=512M
export HEAP_NEWSIZE=128M' > /etc/default/cassandra

echo 'export MAX_HEAP_SIZE=512M
export HEAP_NEWSIZE=128M' > /etc/sysconfig/janusgraph
