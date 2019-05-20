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
    class Append < Command
      def help
        return <<-EOF
Appends a cell 'value' at specified table/row/column coordinates.

  hbase> append 't1', 'r1', 'c1', 'value', ATTRIBUTES=>{'mykey'=>'myvalue'}
  hbase> append 't1', 'r1', 'c1', 'value', {VISIBILITY=>'PRIVATE|SECRET'}

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding command would be:

  hbase> t.append 'r1', 'c1', 'value', ATTRIBUTES=>{'mykey'=>'myvalue'}
  hbase> t.append 'r1', 'c1', 'value', {VISIBILITY=>'PRIVATE|SECRET'}
EOF
      end

      def command(table, row, column, value, args={})
        append(table(table), row, column, value, args)
      end

      def append(table, row, column, value, args={})
      	format_simple_command do
        	table._append_internal(row, column, value, args)
        end
      end
    end
  end
end

#add incr comamnd to Table
::Hbase::Table.add_shell_command("append")
