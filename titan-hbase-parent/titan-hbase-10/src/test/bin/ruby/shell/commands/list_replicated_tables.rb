#
# Copyright The Apache Software Foundation
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
    class ListReplicatedTables< Command
      def help
        return <<-EOF
List all the tables and column families replicated from this cluster

  hbase> list_replicated_tables
  hbase> list_replicated_tables 'abc.*'
EOF
      end

      def command(regex = ".*")
        now = Time.now

        formatter.header([ "TABLE:COLUMNFAMILY", "ReplicationType" ], [ 32 ])
        list = replication_admin.list_replicated_tables(regex)
        list.each do |e|
          if e.get(org.apache.hadoop.hbase.client.replication.ReplicationAdmin::REPLICATIONTYPE) == org.apache.hadoop.hbase.client.replication.ReplicationAdmin::REPLICATIONGLOBAL
             replicateType = "GLOBAL"
          else
             replicateType = "unknown"
          end
          formatter.row([e.get(org.apache.hadoop.hbase.client.replication.ReplicationAdmin::TNAME) + ":" + e.get(org.apache.hadoop.hbase.client.replication.ReplicationAdmin::CFNAME), replicateType], true, [32])
        end
        formatter.footer(now)
      end
    end
  end
end
