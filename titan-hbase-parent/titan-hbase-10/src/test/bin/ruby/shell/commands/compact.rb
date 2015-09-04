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
    class Compact < Command
      def help
        return <<-EOF
          Compact all regions in passed table or pass a region row
          to compact an individual region. You can also compact a single column
          family within a region.
          Examples:
          Compact all regions in a table:
          hbase> compact 'ns1:t1'
          hbase> compact 't1'
          Compact an entire region:
          hbase> compact 'r1'
          Compact only a column family within a region:
          hbase> compact 'r1', 'c1'
          Compact a column family within a table:
          hbase> compact 't1', 'c1'
        EOF
      end

      def command(table_or_region_name, family = nil)
        format_simple_command do
          admin.compact(table_or_region_name, family)
        end
      end
    end
  end
end
