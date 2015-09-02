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
    class RemovePeerTableCFs < Command
      def help
        return <<-EOF
Remove a table / table-cf from the table-cfs config for the specified peer
Examples:

  # Remove a table / table-cf from the replicable table-cfs for a peer
  hbase> remove_peer_tableCFs '2', "table1"
  hbase> remove_peer_tableCFs '2', "table1:cf1"

EOF
      end

      def command(id, table_cfs)
        format_simple_command do
          replication_admin.remove_peer_tableCFs(id, table_cfs)
        end
      end
    end
  end
end
