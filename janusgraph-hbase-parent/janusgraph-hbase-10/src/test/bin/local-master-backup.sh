#!/usr/bin/env bash
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
# This is used for starting multiple masters on the same machine.
# run it from hbase-dir/ just like 'bin/hbase'
# Supports up to 10 masters (limitation = overlapping ports)

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin" >/dev/null && pwd`

if [ $# -lt 2 ]; then
  S=`basename "${BASH_SOURCE-$0}"`
  echo "Usage: $S [--config <conf-dir>] [start|stop] offset(s)"
  echo ""
  echo "    e.g. $S start 1"
  exit
fi

. "$bin"/hbase-config.sh

# sanity check: make sure your master opts don't use ports [i.e. JMX/DBG]
export HBASE_MASTER_OPTS=" "

run_master () {
  DN=$2
  export HBASE_IDENT_STRING="$USER-$DN"
  HBASE_MASTER_ARGS="\
    -D hbase.master.info.port=`expr 16010 + $DN` \
    -D hbase.regionserver.port=`expr 16020 + $DN` \
    -D hbase.regionserver.info.port=`expr 16030 + $DN` \
    --backup"
  "$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" $1 master $HBASE_MASTER_ARGS
}

cmd=$1
shift;

for i in $*
do
  if [[ "$i" =~ ^[0-9]+$ ]]; then
   run_master $cmd $i
  else
   echo "Invalid argument"
  fi
done
