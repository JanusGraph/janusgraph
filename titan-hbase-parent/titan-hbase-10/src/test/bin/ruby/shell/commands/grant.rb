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
    class Grant < Command
      def help
        return <<-EOF
Grant users specific rights.
Syntax : grant <user> <permissions> [<@namespace> [<table> [<column family> [<column qualifier>]]]

permissions is either zero or more letters from the set "RWXCA".
READ('R'), WRITE('W'), EXEC('X'), CREATE('C'), ADMIN('A')

Note: Groups and users are granted access in the same way, but groups are prefixed with an '@' 
      character. In the same way, tables and namespaces are specified, but namespaces are 
      prefixed with an '@' character.

For example:

    hbase> grant 'bobsmith', 'RWXCA'
    hbase> grant '@admins', 'RWXCA'
    hbase> grant 'bobsmith', 'RWXCA', '@ns1'
    hbase> grant 'bobsmith', 'RW', 't1', 'f1', 'col1'
    hbase> grant 'bobsmith', 'RW', 'ns1:t1', 'f1', 'col1'
EOF
      end

      def command(*args)

        # command form is ambiguous at first argument
        table_name = user = args[0]
        raise(ArgumentError, "First argument should be a String") unless user.kind_of?(String)

        if args[1].kind_of?(String)

          # Original form of the command
          #     user in args[0]
          #     permissions in args[1]
          #     table_name in args[2]
          #     family in args[3] or nil
          #     qualifier in args[4] or nil

          permissions = args[1]
          raise(ArgumentError, "Permissions are not of String type") unless permissions.kind_of?(
            String)
          table_name = family = qualifier = nil
          table_name = args[2] # will be nil if unset
          if not table_name.nil?
            raise(ArgumentError, "Table name is not of String type") unless table_name.kind_of?(
              String)
            family = args[3]     # will be nil if unset
            if not family.nil?
              raise(ArgumentError, "Family is not of String type") unless family.kind_of?(String)
              qualifier = args[4]  # will be nil if unset
              if not qualifier.nil?
                raise(ArgumentError, "Qualifier is not of String type") unless qualifier.kind_of?(
                  String)
              end
            end
          end
          format_simple_command do
            security_admin.grant(user, permissions, table_name, family, qualifier)
          end

        elsif args[1].kind_of?(Hash)

          # New form of the command, a cell ACL update
          #    table_name in args[0], a string
          #    a Hash mapping users (or groups) to permisisons in args[1]
          #    a Hash argument suitable for passing to Table#_get_scanner in args[2]
          # Useful for feature testing and debugging.

          permissions = args[1]
          raise(ArgumentError, "Permissions are not of Hash type") unless permissions.kind_of?(Hash)
          scan = args[2]
          raise(ArgumentError, "Scanner specification is not a Hash") unless scan.kind_of?(Hash)

          t = table(table_name)
          now = Time.now
          scanner = t._get_scanner(scan)
          count = 0
          iter = scanner.iterator
          while iter.hasNext
            row = iter.next
            row.list.each do |cell|
              put = org.apache.hadoop.hbase.client.Put.new(row.getRow)
              put.add(cell)
              t.set_cell_permissions(put, permissions)
              t.table.put(put)
            end
            count += 1
          end
          formatter.footer(now, count)

        else
          raise(ArgumentError, "Second argument should be a String or Hash")
        end

      end
    end
  end
end
