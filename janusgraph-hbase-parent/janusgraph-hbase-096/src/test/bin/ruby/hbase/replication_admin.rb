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

include Java

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class RepAdmin
    include HBaseConstants

    def initialize(configuration, formatter)
      @replication_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(configuration)
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    # Add a new peer cluster to replicate to
    def add_peer(id, cluster_key)
      @replication_admin.addPeer(id, cluster_key)
    end

    #----------------------------------------------------------------------------------------------
    # Remove a peer cluster, stops the replication
    def remove_peer(id)
      @replication_admin.removePeer(id)
    end


    #---------------------------------------------------------------------------------------------
    # Show replcated tables/column families, and their ReplicationType
    def list_replicated_tables
       @replication_admin.listReplicated()
    end

    #----------------------------------------------------------------------------------------------
    # List all peer clusters
    def list_peers
      @replication_admin.listPeers
    end

    #----------------------------------------------------------------------------------------------
    # Get peer cluster state
    def get_peer_state(id)
      @replication_admin.getPeerState(id) ? "ENABLED" : "DISABLED"
    end

    #----------------------------------------------------------------------------------------------
    # Restart the replication stream to the specified peer
    def enable_peer(id)
      @replication_admin.enablePeer(id)
    end

    #----------------------------------------------------------------------------------------------
    # Stop the replication stream to the specified peer
    def disable_peer(id)
      @replication_admin.disablePeer(id)
    end
  end
end
