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
         'https://debian.datastax.com/debian/repo_key' \
         'https://aureliuspkg.s3.amazonaws.com/keys/aurelius.asc' ; do
    echo "Downloading key from $u"
    wget -O - "$u" | apt-key add -
done
echo "Keys installed"

SOURCES_D=/etc/apt/sources.list.d
echo "Adding apt repository files to $SOURCES_D"
( set -x 
  echo 'deb http://aureliuspkg.s3.amazonaws.com/deb unstable main' > "$SOURCES_D"/aurelius.sources.list
  echo 'deb https://debian.datastax.com/community stable main' > "$SOURCES_D"/cassandra.sources.list
  echo 'deb http://packages.elasticsearch.org/elasticsearch/1.0/debian stable main' > "$SOURCES_D"/elasticsearch.sources.list )

echo "Installing Cassandra, ES, and JanusGraph with apt-get"
apt-get update
apt-get install cassandra=2.0.7 elasticsearch=1.0.3 janusgraph

# Reduce Cassandra and Gremlin Server/JanusGraph heapsizes
echo 'export MAX_HEAP_SIZE=512M
export HEAP_NEWSIZE=128M' > /etc/default/cassandra

echo 'export MAX_HEAP_SIZE=512M
export HEAP_NEWSIZE=128M' > /etc/default/janusgraph
