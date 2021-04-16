#!/bin/bash
# Copyright 2020 JanusGraph Authors
# Copyright 2015-2019 The Apache Software Foundation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file is based on the work of TinkerPop's gremlin-server.sh, see 
# https://github.com/apache/tinkerpop/blob/master/gremlin-server/src/main/bin/gremlin-server.sh.

### BEGIN INIT INFO
# Provides:          janusgraph-server
# Required-Start:    $remote_fs $syslog $network
# Required-Stop:     $remote_fs $syslog $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: JanusGraph Server
# Description:       JanusGraph Server
# chkconfig:         2345 98 01
### END INIT INFO

[[ -n "$DEBUG" ]] && set -x

SOURCE="$0"
while [[ -h "$SOURCE" ]]; do
  cd -P "$( dirname "$SOURCE" )" || exit 1
  DIR="$(pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
cd -P "$( dirname "$SOURCE" )" || exit 1
JANUSGRAPH_BIN="$(pwd)"
[[ -n "$DEBUG" ]] && set -x

if [[ -z "$JANUSGRAPH_HOME" ]]; then
  cd ..
  JANUSGRAPH_HOME="$(pwd)"
fi

if [[ -z "$JANUSGRAPH_CONF" ]] ; then
  JANUSGRAPH_CONF="$JANUSGRAPH_HOME/conf"
fi

if [[ -z "$LOG_DIR" ]] ; then
  LOG_DIR="$JANUSGRAPH_HOME/logs"
fi

if [[ -z "$LOG_FILE" ]]; then
  LOG_FILE="$LOG_DIR/janusgraph.log"
fi

if [[ -z "$PID_DIR" ]] ; then
  PID_DIR="$JANUSGRAPH_HOME/run"
fi

if [[ -z "$PID_FILE" ]]; then
  PID_FILE="$PID_DIR/janusgraph.pid"
fi

if [[ -z "$JANUSGRAPH_YAML" ]]; then
  JANUSGRAPH_YAML="$JANUSGRAPH_CONF/gremlin-server/gremlin-server.yaml"
fi

if [[ ! -r "$JANUSGRAPH_YAML" ]]; then
  # try relative to conf
  JANUSGRAPH_YAML="$JANUSGRAPH_CONF/gremlin-server/gremlin-server.yaml"
  if [[ ! -r "$JANUSGRAPH_YAML" ]]; then
    echo WARNING: $JANUSGRAPH_YAML is unreadable
  fi
fi

# Set $JANUSGRAPH_LIB to $JANUSGRAPH_HOME/lib if unset
if [[ -z "$JANUSGRAPH_LIB" ]]; then
  JANUSGRAPH_LIB="$JANUSGRAPH_HOME/lib"
fi

# absolute file path requires 'file:'
LOG4J_CONF="file:$JANUSGRAPH_CONF/log4j-server.properties"

# Find Java
if [[ "$JAVA_HOME" = "" ]] ; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

COLLECTED_JAVA_OPTIONS_FILE=""

# Read user-defined JVM options from jvm.options file
if [[ -z "$JAVA_OPTIONS_FILE" ]]; then
  JAVA_OPTIONS_FILE="$JANUSGRAPH_CONF/jvm.options"
fi
if [[ -f "$JAVA_OPTIONS_FILE" ]]; then
  for opt in `grep "^-" $JAVA_OPTIONS_FILE`
  do
    COLLECTED_JAVA_OPTIONS_FILE="$JAVA_OPTIONS_FILE_CURATED $opt"
  done
fi

JAVA_OPTIONS="$COLLECTED_JAVA_OPTIONS_FILE $JAVA_OPTIONS -javaagent:$JANUSGRAPH_LIB/jamm-0.3.0.jar"

# Build Java CLASSPATH
if [[ -z "$CP" ]];then
  # Initialize classpath to $JANUSGRAPH_CFG
  CP="${JANUSGRAPH_CONF}"
  # Add the slf4j-log4j12 binding
  CP="$CP":$(find -L $JANUSGRAPH_LIB -name 'slf4j-log4j12*.jar' | sort | tr '\n' ':')
  # Add the jars in $JANUSGRAPH_HOME/lib that start with "janusgraph"
  CP="$CP":$(find -L $JANUSGRAPH_LIB -name 'janusgraph*.jar' | sort | tr '\n' ':')
  # Add the remaining jars in $JANUSGRAPH_HOME/lib.
  CP="$CP":$(find -L $JANUSGRAPH_LIB -name '*.jar' \
                  \! -name 'janusgraph*' \
                  \! -name 'slf4j-log4j12*.jar' | sort | tr '\n' ':')
  # Add the jars in $BIN/../ext (at any subdirectory depth)
  CP="$CP":$( find -L "$JANUSGRAPH_HOME"/ext -mindepth 1 -maxdepth 1 -type d | \
        sort | sed 's/$/\/plugin\/*/' | tr '\n' ':' )
fi

CLASSPATH="${CLASSPATH:-}:$CP"

JANUSGRAPH_SERVER_CMD=org.janusgraph.graphdb.server.JanusGraphServer


isRunning() {
  if [[ -r "$PID_FILE" ]] ; then
    PID=$(cat "$PID_FILE")
    ps -p "$PID" &> /dev/null
    return $?
  else
    return 1
  fi
}

status() {
  isRunning
  RUNNING=$?
    if [[ $RUNNING -gt 0 ]]; then
      echo Server not running
    else
      echo Server running with PID $(cat "$PID_FILE")
    fi
}

stop() {
  isRunning
  RUNNING=$?
  if [[ $RUNNING -gt 0 ]]; then
    echo Server not running
    rm -f "$PID_FILE"
  else
    kill "$PID" &> /dev/null || { echo "Unable to kill server [$PID]"; exit 1; }
    for i in $(seq 1 60); do
      ps -p "$PID" &> /dev/null || { echo "Server stopped [$PID]"; rm -f "$PID_FILE"; return 0; }
      [[ $i -eq 30 ]] && kill "$PID" &> /dev/null
      sleep 1
    done
    echo "Unable to kill server [$PID]";
    exit 1;
  fi
}

start() {
  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server already running with PID $(cat "$PID_FILE").
    exit 1
  fi

  if [[ -z "$RUNAS" ]]; then

    mkdir -p "$LOG_DIR" &>/dev/null
    if [[ ! -d "$LOG_DIR" ]]; then
      echo ERROR: LOG_DIR $LOG_DIR does not exist and could not be created.
      exit 1
    fi

    mkdir -p "$PID_DIR" &>/dev/null
    if [[ ! -d "$PID_DIR" ]]; then
      echo ERROR: PID_DIR $PID_DIR does not exist and could not be created.
      exit 1
    fi

    $JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CLASSPATH $JANUSGRAPH_SERVER_CMD "$JANUSGRAPH_YAML" >> "$LOG_FILE" 2>&1 &
    PID=$!
    disown $PID
    echo $PID > "$PID_FILE"
  else

    su -c "mkdir -p $LOG_DIR &>/dev/null"  "$RUNAS"
    if [[ ! -d "$LOG_DIR" ]]; then
      echo ERROR: LOG_DIR $LOG_DIR does not exist and could not be created.
      exit 1
    fi

    su -c "mkdir -p $PID_DIR &>/dev/null"  "$RUNAS"
    if [[ ! -d "$PID_DIR" ]]; then
      echo ERROR: PID_DIR $PID_DIR does not exist and could not be created.
      exit 1
    fi

    su -c "$JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CLASSPATH $JANUSGRAPH_SERVER_CMD \"$JANUSGRAPH_YAML\" >> \"$LOG_FILE\" 2>&1 & echo \$! "  "$RUNAS" > "$PID_FILE"
    chown "$RUNAS" "$PID_FILE"
  fi

  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server started $(cat "$PID_FILE")
    exit 0
  else
    echo Server failed
    exit 1
  fi

}

startForeground() {
  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server already running with PID $(cat "$PID_FILE").
    exit 1
  fi

  if [[ -z "$RUNAS" ]]; then
    exec $JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CLASSPATH $JANUSGRAPH_SERVER_CMD "$JANUSGRAPH_YAML"
    exit 0
  else
    echo Starting in foreground not supported with RUNAS
    exit 1
  fi

}
printUsage() {
  echo "Usage: $0 {start|stop|restart|status|console|usage <group> <artifact> <version>|<conf file>}"
  echo
  echo "    start           Start the server in the background using conf/gremlin-server/gremlin-server.yaml as the"
  echo "                    default configuration file"
  echo "    stop            Stop the server"
  echo "    restart         Stop and start the server"
  echo "    status          Check if the server is running"
  echo "    console         Start the server in the foreground using conf/gremlin-server/gremlin-server.yaml as the"
  echo "                    default configuration file"
  echo "    usage           Print out this help message"
  echo
  echo "If using a custom YAML configuration file then specify it as the only argument for a JanusGraph"
  echo "Server to run in the foreground or specify it via the JANUSGRAPH_YAML environment variable."
  echo
}

case "$1" in
  status)
    status
    ;;
  restart)
    stop
    start
    ;;
  start)
    start
    ;;
  stop)
    stop
    ;;
  console)
    startForeground
    ;;
  help|usage)
    printUsage
    exit 1
    ;;
  *)
    if [[ -n "$1" ]] ; then
      if [[ -r "$1" ]]; then
        JANUSGRAPH_YAML="$1"
        startForeground
      elif [[ -r "$JANUSGRAPH_HOME/$1" ]] ; then
        JANUSGRAPH_YAML="$JANUSGRAPH_HOME/$1"
        startForeground
      fi
      echo Configuration file not found.
    fi
    printUsage
    exit 1
    ;;
esac
