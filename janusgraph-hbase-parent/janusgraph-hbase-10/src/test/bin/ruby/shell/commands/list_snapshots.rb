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

require 'time'

module Shell
  module Commands
    class ListSnapshots < Command
      def help
        return <<-EOF
List all snapshots taken (by printing the names and relative information).
Optional regular expression parameter could be used to filter the output
by snapshot name.

Examples:
  hbase> list_snapshots
  hbase> list_snapshots 'abc.*'
EOF
      end

      def command(regex = ".*")
        now = Time.now
        formatter.header([ "SNAPSHOT", "TABLE + CREATION TIME"])

        list = admin.list_snapshot(regex)
        list.each do |snapshot|
          creation_time = Time.at(snapshot.getCreationTime() / 1000).to_s
          formatter.row([ snapshot.getName, snapshot.getTable + " (" + creation_time + ")" ])
        end

        formatter.footer(now, list.size)
        return list.map { |s| s.getName() }
      end
    end
  end
end
