#!/usr/bin/env bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
#
# cleans hbase related data from zookeeper and hdfs if no hbase process is alive.
#
# Environment Variables
#
#   HBASE_REGIONSERVERS    File naming remote hosts.
#     Default is ${HADOOP_CONF_DIR}/regionservers
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HBASE_CONF_DIR  Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HBASE_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HBASE_SLAVE_TIMEOUT Seconds to wait for timing out a remote command.
#   HBASE_SSH_OPTS Options passed to ssh when running remote commands.
#

usage="Usage: hbase-cleanup.sh (--cleanZk|--cleanHdfs|--cleanAll|--cleanAcls)"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# This will set HBASE_HOME, etc.
. "$bin"/hbase-config.sh

case $1 in
  --cleanZk|--cleanHdfs|--cleanAll|--cleanAcls)
    matches="yes" ;;
  *) ;;
esac
if [ $# -ne 1 -o "$matches" = "" ]; then
  echo $usage
  exit 1;
fi

format_option=$1;

zparent=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.parent`
if [ "$zparent" == "null" ]; then zparent="/hbase"; fi

hrootdir=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool hbase.rootdir`
if [ "$hrootdir" == "null" ]; then hrootdir="file:///tmp/hbase-${USER}/hbase"; fi

check_for_znodes() {
  command=$1;
  case $command in
    regionservers)
      zchild=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.rs`
      if [ "$zchild" == "null" ]; then zchild="rs"; fi
      ;;
    backupmasters)
      zchild=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.backup.masters`
      if [ "$zchild" == "null" ]; then zchild="backup-masters"; fi
      ;;
  esac
  znodes=`"$bin"/hbase zkcli ls $zparent/$zchild 2>&1 | tail -1 | sed "s/\[//" | sed "s/\]//"`
  if [ "$znodes" != "" ]; then
    echo -n "ZNode(s) [${znodes}] of $command are not expired. Exiting without cleaning hbase data."
    echo #force a newline	
    exit 1;
  else
    echo -n "All ZNode(s) of $command are expired."
  fi
  echo #force a newline
}

execute_zk_command() {
  command=$1;
  "$bin"/hbase zkcli $command 2>&1
}

execute_hdfs_command() {
  command=$1;
  "$bin"/hbase org.apache.hadoop.fs.FsShell $command 2>&1
}

execute_clean_acls() {
  command=$1;
  "$bin"/hbase org.apache.hadoop.hbase.zookeeper.ZkAclReset $command 2>&1
}

clean_up() {
  case $1 in
  --cleanZk) 
    execute_zk_command "rmr ${zparent}";
    ;;
  --cleanHdfs)
    execute_hdfs_command "-rmr ${hrootdir}"
    ;;
  --cleanAll)
    execute_zk_command "rmr ${zparent}";
    execute_hdfs_command "-rmr ${hrootdir}"
    ;;
  --cleanAcls)
    execute_clean_acls;
    ;;
  *)
    ;;
  esac	
}

check_znode_exists() {
  command=$1
  "$bin"/hbase zkcli stat $command 2>&1 | grep "Node does not exist\|Connection refused"
}

check_znode_exists $zparent
if [ $? -ne 0 ]; then
  # make sure the online region server(s) znode(s) have been deleted before continuing
  check_for_znodes regionservers
  # make sure the backup master(s) znode(s) has been deleted before continuing
  check_for_znodes backupmasters
  # make sure the master znode has been deleted before continuing
  zmaster=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.master`
  if [ "$zmaster" == "null" ]; then zmaster="master"; fi
    zmaster=$zparent/$zmaster
    check_znode_exists $zmaster
  if [ $? -ne 0 ]; then
    echo -n "Master ZNode is not expired. Exiting without cleaning hbase data."
    echo #force a new line
    exit 1
  else
    echo "Active Master ZNode also expired."
  fi
  echo #force a newline
else
  echo "HBase parent znode ${zparent} does not exist."
fi

# cleans zookeeper and/or hdfs data.
clean_up $format_option
