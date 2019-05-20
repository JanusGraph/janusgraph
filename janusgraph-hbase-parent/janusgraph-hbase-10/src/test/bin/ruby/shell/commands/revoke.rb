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
    class Revoke < Command
      def help
        return <<-EOF
Revoke a user's access rights.
Syntax : revoke <user> [<@namespace> [<table> [<column family> [<column qualifier>]]]]

Note: Groups and users access are revoked in the same way, but groups are prefixed with an '@' 
      character. In the same way, tables and namespaces are specified, but namespaces are 
      prefixed with an '@' character.

For example:

    hbase> revoke 'bobsmith'
    hbase> revoke '@admins'
    hbase> revoke 'bobsmith', '@ns1'
    hbase> revoke 'bobsmith', 't1', 'f1', 'col1'
    hbase> revoke 'bobsmith', 'ns1:t1', 'f1', 'col1'
EOF
      end

      def command(user, table_name=nil, family=nil, qualifier=nil)
        format_simple_command do
          security_admin.revoke(user, table_name, family, qualifier)
        end
      end
    end
  end
end
