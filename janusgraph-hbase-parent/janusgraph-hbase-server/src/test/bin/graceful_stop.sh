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

# Move regions off a server then stop it.  Optionally restart and reload.
# Turn off the balancer before running this script.
function usage {
  echo "Usage: graceful_stop.sh [--config <conf-dir>] [-d] [-e] [--restart [--reload]] [--thrift] [--rest] <hostname>"
  echo " thrift         If we should stop/start thrift before/after the hbase stop/start"
  echo " rest           If we should stop/start rest before/after the hbase stop/start"
  echo " restart        If we should restart after graceful stop"
  echo " reload         Move offloaded regions back on to the restarted server"
  echo " d|debug        Print helpful debug information"
  echo " maxthreads xx  Limit the number of threads used by the region mover. Default value is 1."
  echo " hostname       Hostname of server we are to stop"
  echo " e|failfast     Set -e so exit immediately if any command exits with non-zero status"
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`
# This will set HBASE_HOME, etc.
. "$bin"/hbase-config.sh
# Get arguments
restart=
reload=
debug=
thrift=
rest=
maxthreads=1
failfast=
while [ $# -gt 0 ]
do
  case "$1" in
    --thrift)  thrift=true; shift;;
    --rest)  rest=true; shift;;
    --restart)  restart=true; shift;;
    --reload)   reload=true; shift;;
    --failfast | -e)  failfast=true; shift;;
    --debug | -d)  debug="--debug"; shift;;
    --maxthreads) shift; maxthreads=$1; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;	# terminate while loop
  esac
done

# "$@" contains the rest. Must be at least the hostname left.
if [ $# -lt 1 ]; then
  usage
fi

# Emit a log line w/ iso8901 date prefixed
log() {
  echo `date +%Y-%m-%dT%H:%M:%S` $1
}

# See if we should set fail fast before we do anything.
if [ "$failfast" != "" ]; then
  log "Set failfast, will exit immediately if any command exits with non-zero status"
  set -e
fi

hostname=$1
filename="/tmp/$hostname"

local=false
localhostname=`/bin/hostname`

if [ "$localhostname" == "$hostname" ]; then
  local=true
fi

log "Disabling load balancer"
HBASE_BALANCER_STATE=`echo 'balance_switch false' | "$bin"/hbase --config ${HBASE_CONF_DIR} shell | tail -3 | head -1`
log "Previous balancer state was $HBASE_BALANCER_STATE"

log "Unloading $hostname region(s)"
HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.jruby.Main "$bin"/region_mover.rb --file=$filename $debug --maxthreads=$maxthreads unload $hostname
log "Unloaded $hostname region(s)"

# Stop the server(s). Have to put hostname into its own little file for hbase-daemons.sh
hosts="/tmp/$(basename $0).$$.tmp"
echo $hostname >> $hosts
if [ "$thrift" != "" ]; then
  log "Stopping thrift"
  if [ "$local" ]; then
    "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} stop thrift
  else
    "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} stop thrift
  fi
fi
if [ "$rest" != "" ]; then
  log "Stopping rest"
  if [ "$local" ]; then
    "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} stop rest
  else
    "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} stop rest
  fi
fi
log "Stopping regionserver"
if [ "$local" ]; then
  "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} stop regionserver
else
  "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} stop regionserver
fi
if [ "$restart" != "" ]; then
  log "Restarting regionserver"
  if [ "$local" ]; then
    "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} start regionserver
  else
    "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} start regionserver
  fi
  if [ "$thrift" != "" ]; then
    log "Restarting thrift"
    # -b 0.0.0.0 says listen on all interfaces rather than just default.
    if [ "$local" ]; then
      "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} start thrift -b 0.0.0.0
    else
      "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} start thrift -b 0.0.0.0
    fi
  fi
  if [ "$rest" != "" ]; then
    log "Restarting rest"
    if [ "$local" ]; then
      "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} start rest
    else
      "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} start rest
    fi
  fi
  if [ "$reload" != "" ]; then
    log "Reloading $hostname region(s)"
    HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.jruby.Main "$bin"/region_mover.rb --file=$filename $debug --maxthreads=$maxthreads load $hostname
    log "Reloaded $hostname region(s)"
  fi
fi

# Restore balancer state
if [ $HBASE_BALANCER_STATE != "false" ]; then
  log "Restoring balancer state to " $HBASE_BALANCER_STATE
  echo "balance_switch $HBASE_BALANCER_STATE" | "$bin"/hbase --config ${HBASE_CONF_DIR} shell &> /dev/null
fi

# Cleanup tmp files.
trap "rm -f  "/tmp/$(basename $0).*.tmp" &> /dev/null" EXIT
