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
    class Assign < Command
      def help
        return <<-EOF
Assign a region. Use with caution. If region already assigned,
this command will do a force reassign. For experts only.
Examples:

  hbase> assign 'REGIONNAME'
  hbase> assign 'ENCODED_REGIONNAME'
EOF
      end

      def command(region_name)
        format_simple_command do
          admin.assign(region_name)
        end
      end
    end
  end
end
