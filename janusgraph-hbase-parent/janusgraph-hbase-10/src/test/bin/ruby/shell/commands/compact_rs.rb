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
    class CompactRs < Command
      def help
        return <<-EOF
          Compact all regions on passed regionserver.
          Examples:
          Compact all regions on a regionserver:
          hbase> compact_rs 'host187.example.com,60020'
          or
          hbase> compact_rs 'host187.example.com,60020,1289493121758'
          Major compact all regions on a regionserver:
          hbase> compact_rs 'host187.example.com,60020,1289493121758', true
        EOF
      end

      def command(regionserver, major = false)
        format_simple_command do
          admin.compact_regionserver(regionserver, major)
        end
      end
    end
  end
end
