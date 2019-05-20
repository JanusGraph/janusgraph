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
    class Incr < Command
      def help
        return <<-EOF
Increments a cell 'value' at specified table/row/column coordinates.
To increment a cell value in table 'ns1:t1' or 't1' at row 'r1' under column
'c1' by 1 (can be omitted) or 10 do:

  hbase> incr 'ns1:t1', 'r1', 'c1'
  hbase> incr 't1', 'r1', 'c1'
  hbase> incr 't1', 'r1', 'c1', 1
  hbase> incr 't1', 'r1', 'c1', 10
  hbase> incr 't1', 'r1', 'c1', 10, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> incr 't1', 'r1', 'c1', {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> incr 't1', 'r1', 'c1', 10, {VISIBILITY=>'PRIVATE|SECRET'}

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding command would be:

  hbase> t.incr 'r1', 'c1'
  hbase> t.incr 'r1', 'c1', 1
  hbase> t.incr 'r1', 'c1', 10, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> t.incr 'r1', 'c1', 10, {VISIBILITY=>'PRIVATE|SECRET'}
EOF
      end

      def command(table, row, column, value = nil, args = {})
        incr(table(table), row, column, value, args)
      end

      def incr(table, row, column, value = nil, args={})
      	format_simple_command do
          if cnt = table._incr_internal(row, column, value, args)
            puts "COUNTER VALUE = #{cnt}"
          else
            puts "No counter found at specified coordinates"
          end
      	end
      end
    end
  end
end

#add incr comamnd to Table
::Hbase::Table.add_shell_command("incr")
