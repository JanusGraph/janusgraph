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

# Script to recreate all tables from one cluster to another
# To see usage for this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main copy_tables_desc.rb
#

include Java
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.EmptyWatcher
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.conf.Configuration

# Name of this script
NAME = "copy_tables_desc"

# Print usage for this script
def usage
  puts 'Usage: %s.rb master_zookeeper.quorum.peers:clientport:znode_parent slave_zookeeper.quorum.peers:clientport:znode_parent' % NAME
  exit!
end

if ARGV.size != 2
  usage
end

LOG = LogFactory.getLog(NAME)

parts1 = ARGV[0].split(":")

parts2 = ARGV[1].split(":")

c1 = HBaseConfiguration.create()
c1.set(HConstants::ZOOKEEPER_QUORUM, parts1[0])
c1.set("hbase.zookeeper.property.clientPort", parts1[1])
c1.set(HConstants::ZOOKEEPER_ZNODE_PARENT, parts1[2])

admin1 = HBaseAdmin.new(c1)

c2 = HBaseConfiguration.create()
c2.set(HConstants::ZOOKEEPER_QUORUM, parts2[0])
c2.set("hbase.zookeeper.property.clientPort", parts2[1])
c2.set(HConstants::ZOOKEEPER_ZNODE_PARENT, parts2[2])

admin2 = HBaseAdmin.new(c2)

for t in admin1.listTables()
  admin2.createTable(t)
end


puts "All descriptions were copied"
