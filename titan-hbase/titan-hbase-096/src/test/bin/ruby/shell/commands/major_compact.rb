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
    class MajorCompact < Command
      def help
        return <<-EOF
          Run major compaction on passed table or pass a region row
          to major compact an individual region. To compact a single
          column family within a region specify the region name
          followed by the column family name.
          Examples:
          Compact all regions in a table:
          hbase> major_compact 't1'
          Compact an entire region:
          hbase> major_compact 'r1'
          Compact a single column family within a region:
          hbase> major_compact 'r1', 'c1'
          Compact a single column family within a table:
          hbase> major_compact 't1', 'c1'
        EOF
      end

      def command(table_or_region_name, family = nil)
        format_simple_command do
          admin.major_compact(table_or_region_name, family)
        end
      end
    end
  end
end
