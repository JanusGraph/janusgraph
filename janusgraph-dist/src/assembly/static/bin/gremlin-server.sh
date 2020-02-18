#!/bin/bash
# Copyright 2019 JanusGraph Authors
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

# Load Environment Variables from file if present. This is required for System V.
[ -f /etc/default/janusgraph ] && . /etc/default/janusgraph

[[ -n "$DEBUG" ]] && set -x

SOURCE="${BASH_SOURCE[0]}"

# Set JANUSGRAPH_BIN and JANUSGRAPH_HOME if they are unset
if [[ -z "$JANUSGRAPH_HOME" ]] && [[ -z "$JANUSGRAPH_BIN" ]]; then
  while [[ -h "$SOURCE" ]]; do
    JANUSGRAPH_BIN="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$BIN/$SOURCE"
  done
  JANUSGRAPH_BIN="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  cd -P "$( dirname "$SOURCE" )" || exit 1
  cd $JANUSGRAPH_BIN/..
  JANUSGRAPH_HOME="$(pwd)"
elif [[ -z "$JANUSGRAPH_HOME" ]]; then
  cd $JANUSGRAPH_BIN/..
  JANUSGRAPH_HOME="$(pwd)"
elif [[ -z "$JANUSGRAPH_BIN" ]]; then
  JANUSGRAPH_BIN="${JANUSGRAPH_HOME}/bin"
fi

# Set $JANUSGRAPH_CFG to $JANUSGRAPH_HOME/conf/gremlin-server if unset
if [[ -z "$JANUSGRAPH_CFG" ]]; then
  cd -P ${JANUSGRAPH_HOME}/conf/gremlin-server
  JANUSGRAPH_CFG=$(pwd)
fi
# Set $JANUSGRAPH_LIB to $JANUSGRAPH_HOME/lib if unset
if [[ -z "$JANUSGRAPH_LIB" ]]; then
  cd -P $JANUSGRAPH_HOME/lib
  JANUSGRAPH_LIB=$(pwd)
fi
# Set $JANUSGRAPH_EXT to $JANUSGRAPH_HOME/ext if unset
if [[ -z "$JANUSGRAPH_EXT" ]]; then
  cd -P $JANUSGRAPH_HOME/ext
  JANUSGRAPH_EXT=$(pwd)
fi

if [[ -z "$JANUSGRAPH_CP" ]];then
  # Initialize classpath to $JANUSGRAPH_CFG
  JANUSGRAPH_CP="${JANUSGRAPH_CFG}"
  # Add the slf4j-log4j12 binding
  JANUSGRAPH_CP="$JANUSGRAPH_CP":$(find -L $JANUSGRAPH_LIB -name 'slf4j-log4j12*.jar' | sort | tr '\n' ':')
  # Add the jars in $JANUSGRAPH_HOME/lib that start with "janusgraph"
  JANUSGRAPH_CP="$JANUSGRAPH_CP":$(find -L $JANUSGRAPH_LIB -name 'janusgraph*.jar' | sort | tr '\n' ':')
  # Add the remaining jars in $JANUSGRAPH_HOME/lib.
  JANUSGRAPH_CP="$JANUSGRAPH_CP":$(find -L $JANUSGRAPH_LIB -name '*.jar' \
                  \! -name 'janusgraph*' \
                  \! -name 'slf4j-log4j12*.jar' | sort | tr '\n' ':')
  # Add the jars in $BIN/../ext (at any subdirectory depth)
  JANUSGRAPH_CP="$JANUSGRAPH_CP":$(find -L $JANUSGRAPH_EXT -name '*.jar' | sort | tr '\n' ':')
fi

JANUSGRAPH_SERVER_CMD=org.apache.tinkerpop.gremlin.server.GremlinServer

# (Cygwin only) Use ; classpath separator and reformat paths for Windows ("C:\foo")
[[ $(uname) = CYGWIN* ]] && JANUSGRAPH_CP="$(cygpath -p -w "$JANUSGRAPH_CP")"

export CLASSPATH="${CLASSPATH:-}:$JANUSGRAPH_CP"

# Change to $JANUSGRAPH_HOME parent
cd $JANUSGRAPH_HOME

if [[ -z "$JANUSGRAPH_LOG4J_CONF" ]]; then
  JANUSGRAPH_LOG4J_CONF="file:${JANUSGRAPH_HOME}/conf/gremlin-server/log4j-server.properties"
fi

if [[ -z "$JANUSGRAPH_LOGDIR" ]] ; then
  JANUSGRAPH_LOGDIR="$JANUSGRAPH_HOME/logs"
fi

if [[ -z "$JANUSGRAPH_LOG_FILE" ]]; then
  JANUSGRAPH_LOG_FILE="$JANUSGRAPH_LOGDIR/janusgraph.log"
fi

if [[ -z "$JANUSGRAPH_PID_DIR" ]] ; then
  JANUSGRAPH_PID_DIR="$JANUSGRAPH_HOME/run"
fi

if [[ -z "$JANUSGRAPH_PID_FILE" ]]; then
  JANUSGRAPH_PID_FILE=$JANUSGRAPH_PID_DIR/janusgraph.pid
fi

if [[ -z "$JANUSGRAPH_YAML" ]]; then
  JANUSGRAPH_YAML=$JANUSGRAPH_CFG/gremlin-server.yaml
fi

if [[ ! -r "$JANUSGRAPH_YAML" ]]; then
  # fqdn failed, try relative to home
  if [[ -r "$JANUSGRAPH_HOME/$JANUSGRAPH_YAML" ]]; then
    JANUSGRAPH_YAML="$JANUSGRAPH_HOME/$JANUSGRAPH_YAML"
  else
    echo WARNING: Tried "$JANUSGRAPH_YAML" and "${JANUSGRAPH_HOME}/${JANUSGRAPH_YAML}". Neither were readable.
  fi
fi

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS="-Xms32m -Xmx512m -javaagent:$JANUSGRAPH_LIB/jamm-0.3.0.jar -Dgremlin.io.kryoShimService=org.janusgraph.hadoop.serialize.JanusGraphKryoShimService"
fi

isRunning() {
  if [[ -r "$JANUSGRAPH_PID_FILE" ]] ; then
    PID=$(cat "$JANUSGRAPH_PID_FILE")
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
      echo Server running with PID $(cat "$JANUSGRAPH_PID_FILE")
    fi
}

stop() {
  isRunning
  RUNNING=$?
  if [[ $RUNNING -gt 0 ]]; then
    echo Server not running
    rm -f "$JANUSGRAPH_PID_FILE"
  else
    PID=$(cat "$JANUSGRAPH_PID_FILE")
    kill "$PID" &> /dev/null || { echo "Unable to kill server [$PID]"; exit 1; }
    for i in $(seq 1 60); do
      ps -p "$PID" &> /dev/null || { echo "Server stopped [$PID]"; rm -f "$JANUSGRAPH_PID_FILE"; return 0; }
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
    echo Server already running with PID $(cat "$JANUSGRAPH_PID_FILE").
    exit 1
  fi

  if [[ -z "$RUNAS" ]]; then
    mkdir -p "$JANUSGRAPH_LOGDIR" &>/dev/null
    mkdir -p "$JANUSGRAPH_PID_DIR" &>/dev/null
  elif [[ $(uname) = DARWIN* ]]; then
    su "$RUNAS" -c "mkdir -p $JANUSGRAPH_LOGDIR &>/dev/null"
    su "$RUNAS" -c "mkdir -p $PID_DIR &>/dev/null"
  else
    su -c "mkdir -p $JANUSGRAPH_LOGDIR &>/dev/null"  "$RUNAS"
    su -c "mkdir -p $PID_DIR &>/dev/null"  "$RUNAS"
  fi

  if [[ ! -d "$JANUSGRAPH_LOGDIR" ]]; then
    echo ERROR: JANUSGRAPH_LOGDIR $JANUSGRAPH_LOGDIR does not exist and could not be created.
    exit 1
  fi

  if [[ ! -d "$JANUSGRAPH_PID_DIR" ]]; then
    echo ERROR: JANUSGRAPH_PID_DIR $JANUSGRAPH_PID_DIR does not exist and could not be created.
    exit 1
  fi

  if [[ -z "$RUNAS" ]]; then
    $JAVA -Djanusgraph.logdir="$JANUSGRAPH_LOGDIR" -Dlog4j.configuration=$JANUSGRAPH_LOG4J_CONF $JAVA_OPTIONS \
      -cp $JANUSGRAPH_CP:$CLASSPATH $JANUSGRAPH_SERVER_CMD $JANUSGRAPH_YAML >> "$JANUSGRAPH_LOG_FILE" 2>&1 &
    PID=$!
    disown $PID
    echo $PID > "$JANUSGRAPH_PID_FILE"
  elif [[ $(uname) = DARWIN* ]]; then
    su "$RUNAS" -c "$JAVA -Djanusgraph.logdir=${JANUSGRAPH_LOGDIR} -Dlog4j.configuration=$JANUSGRAPH_LOG4J_CONF $JAVA_OPTIONS -cp $JANUSGRAPH_CP:$CLASSPATH $JANUSGRAPH_SERVER_CMD $JANUSGRAPH_YAML >> ${JANUSGRAPH_LOG_FILE} 2>&1 & echo \$! > ${JANUSGRAPH_PID_FILE}"
    chown "$RUNAS" "$PID_FILE"
  else
    su -c "$JAVA -Djanusgraph.logdir=${JANUSGRAPH_LOGDIR} -Dlog4j.configuration=$JANUSGRAPH_LOG4J_CONF $JAVA_OPTIONS -cp $JANUSGRAPH_CP:$CLASSPATH $JANUSGRAPH_SERVER_CMD $JANUSGRAPH_YAML >> ${JANUSGRAPH_LOG_FILE} 2>&1 & echo \$! "  "$RUNAS" > "$PID_FILE"
    chown "$RUNAS" "$PID_FILE"
  fi

  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server started $(cat "$JANUSGRAPH_PID_FILE").
    exit 0
  else
    echo Server failed to Start
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
    $JAVA -Djanusgraph.logdir="$JANUSGRAPH_LOGDIR" -Dlog4j.configuration="$JANUSGRAPH_LOG4J_CONF" $JAVA_OPTIONS -cp $JANUSGRAPH_CP:$CLASSPATH $JANUSGRAPH_SERVER_CMD "$JANUSGRAPH_YAML"
    exit 0
  else
    echo Starting in foreground not supported with RUNAS
    exit 1
  fi
}

install() {
  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server must be stopped before installing.
    exit 1
  fi

  echo Installing dependency $@

  DEPS="$@"
  if [[ -z "$RUNAS" ]]; then
    $JAVA -Djanusgraph.logdir="$JANUSGRAPH_LOGDIR" -Dlog4j.configuration=$JANUSGRAPH_LOG4J_CONF $JAVA_OPTIONS -cp $JANUSGRAPH_CP:$CLASSPATH org.apache.tinkerpop.gremlin.server.util.GremlinServerInstall $DEPS
  elif [[ $(uname) = DARWIN* ]]; then
    su "$RUNAS" -c "$JAVA -Djanusgraph.logdir=\"$JANUSGRAPH_LOGDIR\" -Dlog4j.configuration=$JANUSGRAPH_LOG4J_CONF $JAVA_OPTIONS -cp $JANUSGRAPH_CP:$CLASSPATH org.apache.tinkerpop.gremlin.server.util.GremlinServerInstall $DEPS"
  else
    su -c "$JAVA -Djanusgraph.logdir=\"$JANUSGRAPH_LOGDIR\" -Dlog4j.configuration=$JANUSGRAPH_LOG4J_CONF $JAVA_OPTIONS -cp $JANUSGRAPH_CP:$CLASSPATH org.apache.tinkerpop.gremlin.server.util.GremlinServerInstall $DEPS " "$RUNAS"
  fi
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
  install|i)
    shift
    install "$@"
    ;;
  help|usage)
    echo "Usage: $0 {start|stop|restart|status|install <group> <artifact> <version>|<conf file>}"
    echo
    echo "    start        Start the server in the background using conf/gremlin-server/gremlin-server.yaml as the"
    echo "                 default configuration file"
    echo "    stop         Stop the server"
    echo "    restart      Stop and start the server"
    echo "    status       Check if the server is running"
    echo "    install      Install dependencies"
    echo "    usage        Show this message"
    echo
    echo "If using a custom YAML configuration file then specify it as the only argument for Gremlin"
    echo "Server to run in the foreground or specify it via the JANUSGRAPH_YAML environment variable."
    echo
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
      else
        echo "Configuration file not found."
      fi
    else
      startForeground
    fi
    ;;
esac
