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

# Add or remove servers from draining mode via zookeeper 

require 'optparse'
include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.zookeeper.ZKUtil
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "draining_servers"

# Do command-line parsing
options = {}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: ./hbase org.jruby.Main #{NAME}.rb [options] add|remove|list <hostname>|<host:port>|<servername> ..."
  opts.separator 'Add remove or list servers in draining mode. Can accept either hostname to drain all region servers' +
                 'in that host, a host:port pair or a host,port,startCode triplet. More than one server can be given separated by space'
  opts.on('-h', '--help', 'Display usage information') do
    puts opts
    exit
  end
  options[:debug] = false
  opts.on('-d', '--debug', 'Display extra debug logging') do
    options[:debug] = true
  end
end
optparse.parse!

# Return array of servernames where servername is hostname+port+startcode
# comma-delimited
def getServers(admin)
  serverInfos = admin.getClusterStatus().getServerInfo()
  servers = []
  for server in serverInfos
    servers << server.getServerName()
  end
  return servers
end

def getServerNames(hostOrServers, config)
  ret = []
  
  for hostOrServer in hostOrServers
    # check whether it is already serverName. No need to connect to cluster
    parts = hostOrServer.split(',')
    if parts.size() == 3
      ret << hostOrServer
    else 
      admin = HBaseAdmin.new(config) if not admin
      servers = getServers(admin)

      hostOrServer = hostOrServer.gsub(/:/, ",")
      for server in servers 
        ret << server if server.start_with?(hostOrServer)
      end
    end
  end
  
  admin.close() if admin
  return ret
end

def addServers(options, hostOrServers)
  config = HBaseConfiguration.create()
  servers = getServerNames(hostOrServers, config)
  
  zkw = org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher.new(config, "draining_servers", nil)
  parentZnode = zkw.drainingZNode
  
  begin
    for server in servers
      node = ZKUtil.joinZNode(parentZnode, server)
      ZKUtil.createAndFailSilent(zkw, node)
    end
  ensure
    zkw.close()
  end
end

def removeServers(options, hostOrServers)
  config = HBaseConfiguration.create()
  servers = getServerNames(hostOrServers, config)
  
  zkw = org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher.new(config, "draining_servers", nil)
  parentZnode = zkw.drainingZNode
  
  begin
    for server in servers
      node = ZKUtil.joinZNode(parentZnode, server)
      ZKUtil.deleteNodeFailSilent(zkw, node)
    end
  ensure
    zkw.close()
  end
end

# list servers in draining mode
def listServers(options)
  config = HBaseConfiguration.create()
  
  zkw = org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher.new(config, "draining_servers", nil)
  parentZnode = zkw.drainingZNode

  servers = ZKUtil.listChildrenNoWatch(zkw, parentZnode)
  servers.each {|server| puts server}
end

hostOrServers = ARGV[1..ARGV.size()]

# Create a logger and disable the DEBUG-level annoying client logging
def configureLogging(options)
  apacheLogger = LogFactory.getLog(NAME)
  # Configure log4j to not spew so much
  unless (options[:debug]) 
    logger = org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase")
    logger.setLevel(org.apache.log4j.Level::WARN)
    logger = org.apache.log4j.Logger.getLogger("org.apache.zookeeper")
    logger.setLevel(org.apache.log4j.Level::WARN)
  end
  return apacheLogger
end

# Create a logger and save it to ruby global
$LOG = configureLogging(options)
case ARGV[0]
  when 'add'
    if ARGV.length < 2
      puts optparse
      exit 1
    end
    addServers(options, hostOrServers)
  when 'remove'
    if ARGV.length < 2
      puts optparse
      exit 1
    end
    removeServers(options, hostOrServers)
  when 'list'
    listServers(options)
  else
    puts optparse
    exit 3
end
