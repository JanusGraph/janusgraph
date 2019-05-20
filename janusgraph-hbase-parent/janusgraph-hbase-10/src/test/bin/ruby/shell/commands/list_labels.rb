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
    class ListLabels < Command
      def help
        return <<-EOF
List the visibility labels defined in the system.
Optional regular expression parameter could be used to filter the labels being returned.
Syntax : list_labels

For example:

    hbase> list_labels 'secret.*'
    hbase> list_labels
EOF
      end

      def command(regex = ".*")
        format_simple_command do
          list = visibility_labels_admin.list_labels(regex)
          list.each do |label|
            formatter.row([org.apache.hadoop.hbase.util.Bytes::toStringBinary(label.toByteArray)])
          end
        end
      end
    end
  end
end
