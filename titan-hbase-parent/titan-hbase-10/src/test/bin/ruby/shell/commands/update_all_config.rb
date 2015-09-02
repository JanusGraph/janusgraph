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
    class UpdateAllConfig < Command
      def help
        return <<-EOF
Reload a subset of configuration on all servers in the cluster.  See
http://hbase.apache.org/book.html?dyn_config for more details. Here is how
you would run the command in the hbase shell:
  hbase> update_all_config
EOF
      end

      def command()
        format_simple_command do
          admin.update_all_config()
        end
      end
    end
  end
end
