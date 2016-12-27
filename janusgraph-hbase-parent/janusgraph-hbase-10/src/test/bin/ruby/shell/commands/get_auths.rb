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
    class GetAuths < Command
      def help
        return <<-EOF
Get the visibility labels set for a particular user or group
Syntax : get_auths 'user'

For example:

    hbase> get_auths 'user1'
    hbase> get_auths '@group1'
EOF
      end

      def command(user)
        format_simple_command do
          list = visibility_labels_admin.get_auths(user)
          list.each do |auths|
            formatter.row([org.apache.hadoop.hbase.util.Bytes::toStringBinary(auths.toByteArray)])
          end  
        end
      end
    end
  end
end
