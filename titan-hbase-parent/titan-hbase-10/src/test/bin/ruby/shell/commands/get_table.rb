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
    class GetTable < Command
      def help
        return <<-EOF
Get the given table name and return it as an actual object to
be manipulated by the user. See table.help for more information
on how to use the table.
Eg.

  hbase> t1 = get_table 't1'
  hbase> t1 = get_table 'ns1:t1'

returns the table named 't1' as a table object. You can then do

  hbase> t1.help

which will then print the help for that table.
EOF
      end

      def command(table, *args)
        format_and_return_simple_command do
          table(table)
        end
      end
    end
  end
end
