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
    class DeleteAllSnapshot < Command
      def help
        return <<-EOF
Delete all of the snapshots matching the given regex. Examples:

  hbase> delete_all_snapshot 's.*'

EOF
      end

      def command(regex)
        formatter.header([ "SNAPSHOT", "TABLE + CREATION TIME"])
        list = admin.list_snapshot(regex)
        count = list.size
        list.each do |snapshot|
          creation_time = Time.at(snapshot.getCreationTime() / 1000).to_s
          formatter.row([ snapshot.getName, snapshot.getTable + " (" + creation_time + ")" ])
        end
        puts "\nDelete the above #{count} snapshots (y/n)?" unless count == 0
        answer = 'n'
        answer = gets.chomp unless count == 0
        puts "No snapshots matched the regex #{regex.to_s}" if count == 0
        return unless answer =~ /y.*/i
        format_simple_command do
          admin.delete_all_snapshot(regex)
        end
        list = admin.list_snapshot(regex)
        leftOverSnapshotCount = list.size
        successfullyDeleted = count - leftOverSnapshotCount
        puts "#{successfullyDeleted} snapshots successfully deleted." unless successfullyDeleted == 0
        return if leftOverSnapshotCount == 0
        puts "\nFailed to delete the below #{leftOverSnapshotCount} snapshots."
        formatter.header([ "SNAPSHOT", "TABLE + CREATION TIME"])
        list.each do |snapshot|
          creation_time = Time.at(snapshot.getCreationTime() / 1000).to_s
          formatter.row([ snapshot.getName, snapshot.getTable + " (" + creation_time + ")" ])
        end
      end
    end
  end
end
