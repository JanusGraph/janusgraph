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

java_import org.apache.hadoop.hbase.client.replication.ReplicationAdmin
java_import org.apache.hadoop.hbase.replication.ReplicationPeerConfig
java_import org.apache.hadoop.hbase.util.Bytes
java_import org.apache.hadoop.hbase.zookeeper.ZKUtil
java_import org.apache.hadoop.hbase.TableName

# Wrapper for org.apache.hadoop.hbase.client.replication.ReplicationAdmin

module Hbase
  class RepAdmin
    include HBaseConstants

    def initialize(configuration, formatter)
      @replication_admin = ReplicationAdmin.new(configuration)
      @configuration = configuration
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    # Add a new peer cluster to replicate to
    def add_peer(id, args = {}, peer_tableCFs = nil)
      # make add_peer backwards compatible to take in string for clusterKey and peer_tableCFs
      if args.is_a?(String)
        cluster_key = args
        @replication_admin.addPeer(id, cluster_key, peer_tableCFs)
      elsif args.is_a?(Hash)
        unless peer_tableCFs.nil?
          raise(ArgumentError, "peer_tableCFs should be specified as TABLE_CFS in args")
        end

        endpoint_classname = args.fetch(ENDPOINT_CLASSNAME, nil)
        cluster_key = args.fetch(CLUSTER_KEY, nil)

        # Handle cases where custom replication endpoint and cluster key are either both provided
        # or neither are provided
        if endpoint_classname.nil? and cluster_key.nil?
          raise(ArgumentError, "Either ENDPOINT_CLASSNAME or CLUSTER_KEY must be specified.")
        elsif !endpoint_classname.nil? and !cluster_key.nil?
          raise(ArgumentError, "ENDPOINT_CLASSNAME and CLUSTER_KEY cannot both be specified.")
        end

        # Cluster Key is required for ReplicationPeerConfig for a custom replication endpoint
        if !endpoint_classname.nil? and cluster_key.nil?
          cluster_key = ZKUtil.getZooKeeperClusterKey(@configuration)
        end

        # Optional parameters
        config = args.fetch(CONFIG, nil)
        data = args.fetch(DATA, nil)
        table_cfs = args.fetch(TABLE_CFS, nil)

        # Create and populate a ReplicationPeerConfig
        replication_peer_config = ReplicationPeerConfig.new
        replication_peer_config.set_cluster_key(cluster_key)

        unless endpoint_classname.nil?
          replication_peer_config.set_replication_endpoint_impl(endpoint_classname)
        end

        unless config.nil?
          replication_peer_config.get_configuration.put_all(config)
        end

        unless data.nil?
          # Convert Strings to Bytes for peer_data
          peer_data = replication_peer_config.get_peer_data
          data.each{|key, val|
            peer_data.put(Bytes.to_bytes(key), Bytes.to_bytes(val))
          }
        end

        @replication_admin.add_peer(id, replication_peer_config, table_cfs)
      else
        raise(ArgumentError, "args must be either a String or Hash")
      end
    end

    #----------------------------------------------------------------------------------------------
    # Remove a peer cluster, stops the replication
    def remove_peer(id)
      @replication_admin.removePeer(id)
    end


    #---------------------------------------------------------------------------------------------
    # Show replcated tables/column families, and their ReplicationType
    def list_replicated_tables(regex = ".*")
      pattern = java.util.regex.Pattern.compile(regex)
      list = @replication_admin.listReplicated()
      list.select {|s| pattern.match(s.get(ReplicationAdmin::TNAME))}
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

    #----------------------------------------------------------------------------------------------
    # Show the current tableCFs config for the specified peer
    def show_peer_tableCFs(id)
      @replication_admin.getPeerTableCFs(id)
    end

    #----------------------------------------------------------------------------------------------
    # Set new tableCFs config for the specified peer
    def set_peer_tableCFs(id, tableCFs)
      @replication_admin.setPeerTableCFs(id, tableCFs)
    end

    #----------------------------------------------------------------------------------------------
    # Append a tableCFs config for the specified peer
    def append_peer_tableCFs(id, tableCFs)
      @replication_admin.appendPeerTableCFs(id, tableCFs)
    end

    #----------------------------------------------------------------------------------------------
    # Remove some tableCFs from the tableCFs config of the specified peer
    def remove_peer_tableCFs(id, tableCFs)
      @replication_admin.removePeerTableCFs(id, tableCFs)
    end
    #----------------------------------------------------------------------------------------------
    # Enables a table's replication switch
    def enable_tablerep(table_name)
      tableName = TableName.valueOf(table_name)
      @replication_admin.enableTableRep(tableName)
    end
    #----------------------------------------------------------------------------------------------
    # Disables a table's replication switch
    def disable_tablerep(table_name)
      tableName = TableName.valueOf(table_name)
      @replication_admin.disableTableRep(tableName)
    end
  end
end
