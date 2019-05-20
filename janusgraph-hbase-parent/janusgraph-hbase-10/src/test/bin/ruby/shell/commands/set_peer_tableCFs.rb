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
    class SetPeerTableCFs< Command
      def help
        return <<-EOF
  Set the replicable table-cf config for the specified peer
  Examples:

    # set all tables to be replicable for a peer
    hbase> set_peer_tableCFs '1', ""
    hbase> set_peer_tableCFs '1'
    # set table / table-cf to be replicable for a peer, for a table without
    # an explicit column-family list, all replicable column-families (with
    # replication_scope == 1) will be replicated
    hbase> set_peer_tableCFs '2', "table1; table2:cf1,cf2; table3:cfA,cfB"

  EOF
      end

      def command(id, peer_table_cfs = nil)
        format_simple_command do
          replication_admin.set_peer_tableCFs(id, peer_table_cfs)
        end
      end
    end
  end
end
