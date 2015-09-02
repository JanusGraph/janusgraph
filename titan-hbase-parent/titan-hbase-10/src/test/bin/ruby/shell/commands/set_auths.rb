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
    class SetAuths < Command
      def help
        return <<-EOF
Add a set of visibility labels for a user or group
Syntax : set_auths 'user',[label1, label2]

For example:

    hbase> set_auths 'user1', ['SECRET','PRIVATE']
    hbase> set_auths '@group1', ['SECRET','PRIVATE']
EOF
      end

      def command(user, *args)
        format_simple_command do
          visibility_labels_admin.set_auths(user, args)
        end
      end
    end
  end
end
