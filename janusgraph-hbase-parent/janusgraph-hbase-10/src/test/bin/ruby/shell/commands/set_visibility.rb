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
    class SetVisibility < Command
      def help
        return <<-EOF
Set the visibility expression on one or more existing cells.

Pass table name, visibility expression, and a dictionary containing
scanner specifications.  Scanner specifications may include one or more
of: TIMERANGE, FILTER, STARTROW, STOPROW, TIMESTAMP, or COLUMNS

If no columns are specified, all columns will be included.
To include all members of a column family, leave the qualifier empty as in
'col_family:'.

The filter can be specified in two ways:
1. Using a filterString - more information on this is available in the
Filter Language document attached to the HBASE-4176 JIRA
2. Using the entire package name of the filter.

Examples:

    hbase> set_visibility 't1', 'A|B', {COLUMNS => ['c1', 'c2']}
    hbase> set_visibility 't1', '(A&B)|C', {COLUMNS => 'c1',
        TIMERANGE => [1303668804, 1303668904]}
    hbase> set_visibility 't1', 'A&B&C', {FILTER => "(PrefixFilter ('row2') AND
        (QualifierFilter (>=, 'binary:xyz'))) AND
        (TimestampsFilter ( 123, 456))"}

This command will only affect existing cells and is expected to be mainly
useful for feature testing and functional verification.
EOF
      end

      def command(table, visibility, scan)
        t = table(table)
        now = Time.now
        scanner = t._get_scanner(scan)
        count = 0
        iter = scanner.iterator
        while iter.hasNext
          row = iter.next
          row.list.each do |cell|
            put = org.apache.hadoop.hbase.client.Put.new(row.getRow)
            put.add(cell)
            t.set_cell_visibility(put, visibility)
            t.table.put(put)
          end
          count += 1
        end
        formatter.footer(now, count)
      end

    end
  end
end
