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
    class CloneSnapshot < Command
      def help
        return <<-EOF
Create a new table by cloning the snapshot content. 
There're no copies of data involved.
And writing on the newly created table will not influence the snapshot data.

Examples:
  hbase> clone_snapshot 'snapshotName', 'tableName'
  hbase> clone_snapshot 'snapshotName', 'namespace:tableName'
EOF
      end

      def command(snapshot_name, table)
        format_simple_command do
          admin.clone_snapshot(snapshot_name, table)
        end
      end
    end
  end
end
