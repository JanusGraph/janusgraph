#!/bin/bash
#
#/**
# * Copyright The Apache Software Foundation
# *
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

set -e -u -o pipefail

SCRIPT_NAME=${0##*/}
SCRIPT_DIR=$(cd `dirname $0` && pwd )

print_usage() {
  cat >&2 <<EOT
Usage: $SCRIPT_NAME <options>
Options:
  --kill
    Kill local process-based HBase cluster using pid files.
  --show
    Show HBase processes running on this machine
EOT
  exit 1
}

show_processes() {
  ps -ef | grep -P "(HRegionServer|HMaster|HQuorumPeer) start" | grep -v grep
}

cmd_specified() {
  if [ "$CMD_SPECIFIED" ]; then
    echo "Only one command can be specified" >&2
    exit 1
  fi
  CMD_SPECIFIED=1
}

list_pid_files() {
  LOCAL_CLUSTER_DIR=$SCRIPT_DIR/../../target/local_cluster
  LOCAL_CLUSTER_DIR=$( cd $LOCAL_CLUSTER_DIR && pwd )
  find $LOCAL_CLUSTER_DIR -name "*.pid"
}

if [ $# -eq 0 ]; then
  print_usage
fi

IS_KILL=""
IS_SHOW=""
CMD_SPECIFIED=""

while [ $# -ne 0 ]; do
  case "$1" in
    -h|--help)
      print_usage ;;
    --kill)
      IS_KILL=1 
      cmd_specified ;;
    --show)
      IS_SHOW=1
      cmd_specified ;;
    *)
      echo "Invalid option: $1" >&2
      exit 1
  esac
  shift
done

if [ "$IS_KILL" ]; then
  list_pid_files | \
    while read F; do
      PID=`cat $F`
      echo "Killing pid $PID from file $F"
      # Kill may fail but that's OK, so turn off error handling for a moment.
      set +e
      kill -9 $PID
      set -e
    done
elif [ "$IS_SHOW" ]; then
  PIDS=""
  for F in `list_pid_files`; do
    PID=`cat $F`
    if [ -n "$PID" ]; then
      if [ -n "$PIDS" ]; then
        PIDS="$PIDS,"
      fi
      PIDS="$PIDS$PID"
    fi
  done
  ps -p $PIDS
else
  echo "No command specified" >&2
  exit 1
fi


