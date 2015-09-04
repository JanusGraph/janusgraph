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
    class AppendPeerTableCFs< Command
      def help
        return <<-EOF
Append a replicable table-cf config for the specified peer
Examples:

  # append a table / table-cf to be replicable for a peer
  hbase> append_peer_tableCFs '2', "table4:cfA,cfB"

EOF
      end

      def command(id, table_cfs)
        format_simple_command do
          replication_admin.append_peer_tableCFs(id, table_cfs)
        end
      end
    end
  end
end
