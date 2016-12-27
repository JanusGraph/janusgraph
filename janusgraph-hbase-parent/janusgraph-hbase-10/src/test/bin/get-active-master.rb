#!/usr/bin/env hbase-jruby
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# Prints the hostname of the machine running the active master.

include Java 
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hbase.zookeeper.ZKUtil
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher

# disable debug/info logging on this script for clarity
log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger('org.apache.hadoop.hbase').setLevel(log_level)
org.apache.log4j.Logger.getLogger('org.apache.zookeeper').setLevel(log_level)

config = HBaseConfiguration.create

zk = ZooKeeperWatcher.new(config, 'get-active-master', nil)
begin
  master_address = ZKUtil.getData(zk, zk.masterAddressZNode)
  if master_address
    puts ServerName.parseFrom(master_address).getHostname()
  else
    puts 'Master not running'
  end
ensure
  zk.close()
end

