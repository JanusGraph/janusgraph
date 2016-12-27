#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module Shell
  module Commands
    class AddPeer< Command
      def help
        return <<-EOF
A peer can either be another HBase cluster or a custom replication endpoint. In either case an id
must be specified to identify the peer.

For a HBase cluster peer, a cluster key must be provided and is composed like this:
hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
This gives a full path for HBase to connect to another HBase cluster. An optional parameter for
table column families identifies which column families will be replicated to the peer cluster.
Examples:

  hbase> add_peer '1', "server1.cie.com:2181:/hbase"
  hbase> add_peer '2', "zk1,zk2,zk3:2182:/hbase-prod"
  hbase> add_peer '3', "zk4,zk5,zk6:11000:/hbase-test", "table1; table2:cf1; table3:cf1,cf2"
  hbase> add_peer '4', CLUSTER_KEY => "server1.cie.com:2181:/hbase"
  hbase> add_peer '5', CLUSTER_KEY => "server1.cie.com:2181:/hbase",
    TABLE_CFS => { "table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }

For a custom replication endpoint, the ENDPOINT_CLASSNAME can be provided. Two optional arguments
are DATA and CONFIG which can be specified to set different either the peer_data or configuration
for the custom replication endpoint. Table column families is optional and can be specified with
the key TABLE_CFS.

  hbase> add_peer '6', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint'
  hbase> add_peer '7', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    DATA => { "key1" => 1 }
  hbase> add_peer '8', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    CONFIG => { "config1" => "value1", "config2" => "value2" }
  hbase> add_peer '9', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    DATA => { "key1" => 1 }, CONFIG => { "config1" => "value1", "config2" => "value2" },
  hbase> add_peer '10', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    TABLE_CFS => { "table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }
  hbase> add_peer '11', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    DATA => { "key1" => 1 }, CONFIG => { "config1" => "value1", "config2" => "value2" },
    TABLE_CFS => { "table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }

Note: Either CLUSTER_KEY or ENDPOINT_CLASSNAME must be specified but not both.
EOF
      end

      def command(id, args = {}, peer_tableCFs = nil)
        format_simple_command do
          replication_admin.add_peer(id, args, peer_tableCFs)
        end
      end
    end
  end
end
