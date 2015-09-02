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
    class GetCounter < Command
      def help
        return <<-EOF
Return a counter cell value at specified table/row/column coordinates.
A counter cell should be managed with atomic increment functions on HBase
and the data should be binary encoded (as long value). Example:

  hbase> get_counter 'ns1:t1', 'r1', 'c1'
  hbase> get_counter 't1', 'r1', 'c1'

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding command would be:

  hbase> t.get_counter 'r1', 'c1'
EOF
      end

      def command(table, row, column, dummy = nil)
        get_counter(table(table), row, column)
      end

      def get_counter(table, row, column, dummy = nil)
        if cnt = table._get_counter_internal(row, column)
          puts "COUNTER VALUE = #{cnt}"
        else
          puts "No counter found at specified coordinates"
        end
      end
    end
  end
end

::Hbase::Table.add_shell_command('get_counter')
