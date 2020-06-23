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
# limitations under the License

# Returns the absolute path of this script regardless of symlinks
abs_path() {
    # From: https://stackoverflow.com/a/246128
    #   - To resolve finding the directory after symlinks
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=`abs_path`
GSRV_CONFIG_TAG=cql-es

if [ -z "$JANUSGRAPH_HOME" ]; then
    JANUSGRAPH_HOME="`dirname "$0"`/.."
fi


: ${CASSANDRA_STARTUP_TIMEOUT_S:=60}
: ${CASSANDRA_SHUTDOWN_TIMEOUT_S:=60}

: ${ELASTICSEARCH_STARTUP_TIMEOUT_S:=60}
: ${ELASTICSEARCH_SHUTDOWN_TIMEOUT_S:=60}
: ${ELASTICSEARCH_IP:=127.0.0.1}
: ${ELASTICSEARCH_PORT:=9200}

: ${GSRV_STARTUP_TIMEOUT_S:=60}
: ${GSRV_SHUTDOWN_TIMEOUT_S:=60}
: ${GSRV_IP:=127.0.0.1}
: ${GSRV_PORT:=8182}

: ${SLEEP_INTERVAL_S:=2}
VERBOSE=
COMMAND=

# Locate the jps command.  Check $PATH, then check $JAVA_HOME/bin.
# This does not need to by cygpath'd.
JPS=
for maybejps in jps "${JAVA_HOME}/bin/jps"; do
    type "$maybejps" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        JPS="$maybejps"
        break
    fi
done

if [ -z "$JPS" ]; then
    echo "jps command not found.  Put the JDK's jps binary on the command path." >&2
    exit 1
fi

JANUSGRAPH_FRIENDLY_NAME='JanusGraph-Server'
JANUSGRAPH_CLASS_NAME=org.janusgraph.graphdb.server.JanusGraphServer
ES_FRIENDLY_NAME=Elasticsearch
ES_CLASS_NAME=org.elasticsearch.bootstrap.Elasticsearch
CASSANDRA_FRIENDLY_NAME=Cassandra
CASSANDRA_CLASS_NAME=org.apache.cassandra.service.CassandraDaemon

wait_for_cassandra() {
    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $CASSANDRA_STARTUP_TIMEOUT_S ))
    local statusbinary=

    echo -n 'Running `nodetool statusbinary`'
    while [ $now_s -le $stop_s ]; do
        echo -n .
        # The \r\n deletion bit is necessary for Cygwin compatibility
        statusbinary="`$BIN/../cassandra/bin/nodetool statusbinary 2>/dev/null | tr -d '\n\r'`"
        if [ $? -eq 0 -a 'running' = "$statusbinary" ]; then
            echo ' OK (returned exit status 0 and printed string "running").'
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo " timeout exceeded ($CASSANDRA_STARTUP_TIMEOUT_S seconds)" >&2
    return 1
}


# wait_for_startup friendly_name host port timeout_s
wait_for_startup() {
    local friendly_name="$1"
    local host="$2"
    local port="$3"
    local timeout_s="$4"

    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $timeout_s ))
    local status=

    echo -n "Connecting to $friendly_name ($host:$port)"
    while [ $now_s -le $stop_s ]; do
        echo -n .
        $BIN/checksocket.sh $host $port >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo " OK (connected to $host:$port)."
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo " timeout exceeded ($timeout_s seconds): could not connect to $host:$port" >&2
    return 1
}

# wait_for_shutdown friendly_name class_name timeout_s
wait_for_shutdown() {
    local friendly_name="$1"
    local class_name="$2"
    local timeout_s="$3"

    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $timeout_s ))

    while [ $now_s -le $stop_s ]; do
        status_class "$friendly_name" $class_name >/dev/null
        if [ $? -eq 1 ]; then
            # Class not found in the jps output.  Assume that it stopped.
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo "$friendly_name shutdown timeout exceeded ($timeout_s seconds)" >&2
    return 1
}

start() {
    mkdir -p "$BIN"/../db

    status_class $CASSANDRA_FRIENDLY_NAME $CASSANDRA_CLASS_NAME >/dev/null && status && echo "Stop services before starting" && exit 1
    echo "Forking Cassandra..."
    if [ -n "$VERBOSE" ]; then
        "$BIN"/../cassandra/bin/cassandra || exit 1
    else
        "$BIN"/../cassandra/bin/cassandra >/dev/null 2>&1 || exit 1
    fi
    wait_for_cassandra || {
        echo "See $BIN/../logs/cassandra.log for Cassandra log output."    >&2
        return 1
    }

    status_class $ES_FRIENDLY_NAME $ES_CLASS_NAME >/dev/null && status && echo "Stop services before starting" && exit 1
    echo "Forking Elasticsearch..."
    if [ -n "$VERBOSE" ]; then
        "$BIN"/../elasticsearch/bin/elasticsearch -d
    else
        "$BIN"/../elasticsearch/bin/elasticsearch -d >/dev/null 2>&1
    fi
    wait_for_startup Elasticsearch $ELASTICSEARCH_IP $ELASTICSEARCH_PORT $ELASTICSEARCH_STARTUP_TIMEOUT_S || {
        echo "See $BIN/../logs/elasticsearch.log for Elasticsearch log output."  >&2
        return 1
    }

    status_class $JANUSGRAPH_FRIENDLY_NAME $JANUSGRAPH_CLASS_NAME >/dev/null && status && echo "Stop services before starting" && exit 1
    echo "Forking $JANUSGRAPH_FRIENDLY_NAME..."
    if [ -n "$VERBOSE" ]; then
        "$BIN"/janusgraph-server.sh console conf/gremlin-server/gremlin-server-cql-es.yaml &
    else
        "$BIN"/janusgraph-server.sh console conf/gremlin-server/gremlin-server-cql-es.yaml >/dev/null 2>&1 &
    fi
    wait_for_startup $JANUSGRAPH_FRIENDLY_NAME $GSRV_IP $GSRV_PORT $GSRV_STARTUP_TIMEOUT_S || {
        echo "See $BIN/../logs/janusgraph.log for $JANUSGRAPH_FRIENDLY_NAME log output."  >&2
        return 1
    }
    disown

    echo "Run gremlin.sh to connect." >&2
}

stop() {
    kill_class        $JANUSGRAPH_FRIENDLY_NAME $JANUSGRAPH_CLASS_NAME
    wait_for_shutdown $JANUSGRAPH_FRIENDLY_NAME $JANUSGRAPH_CLASS_NAME $GSRV_SHUTDOWN_TIMEOUT_S
    kill_class        $ES_FRIENDLY_NAME $ES_CLASS_NAME
    wait_for_shutdown $ES_FRIENDLY_NAME $ES_CLASS_NAME $ELASTICSEARCH_SHUTDOWN_TIMEOUT_S
    kill_class        $CASSANDRA_FRIENDLY_NAME $CASSANDRA_CLASS_NAME
    wait_for_shutdown $CASSANDRA_FRIENDLY_NAME $CASSANDRA_CLASS_NAME $CASSANDRA_SHUTDOWN_TIMEOUT_S
}

kill_class() {
    local p=`$JPS -l | grep "$2" | awk '{print $1}'`
    if [ -z "$p" ]; then
        echo "$1 ($2) not found in the java process table"
        return
    fi
    echo "Killing $1 (pid $p)..." >&2
    case "`uname`" in
        CYGWIN*) taskkill /F /PID "$p" ;;
        *)       kill "$p" ;;
    esac
}

status_class() {
    local p=`$JPS -l | grep "$2" | awk '{print $1}'`
    if [ -n "$p" ]; then
        echo "$1 ($2) is running with pid $p"
        return 0
    else
        echo "$1 ($2) does not appear in the java process table"
        return 1
    fi
}

status() {
    status_class $JANUSGRAPH_FRIENDLY_NAME $JANUSGRAPH_CLASS_NAME
    status_class $ES_FRIENDLY_NAME $ES_CLASS_NAME
    status_class $CASSANDRA_FRIENDLY_NAME $CASSANDRA_CLASS_NAME
}

clean() {
    echo -n "Are you sure you want to delete all stored data and logs? [y/N] " >&2
    read response
    if [ "$response" != "y" -a "$response" != "Y" ]; then
        echo "Response \"$response\" did not equal \"y\" or \"Y\".  Canceling clean operation." >&2
        return 0
    fi

    if cd "$BIN"/../db 2>/dev/null; then
        rm -rf cassandra es
        echo "Deleted data in `pwd`" >&2
        cd - >/dev/null
    else
        echo 'Data directory does not exist.' >&2
    fi

    if cd "$BIN"/../logs; then
        rm -f cassandra*.log
        rm -f elasticsearch*.log
        rm -f gremlin-server.log
        echo "Deleted logs in `pwd`" >&2
        cd - >/dev/null
    fi
}

usage() {
    echo "Usage: $0 [options] {start|stop|status|clean}" >&2
    echo " start:  fork Cassandra, ES, and Gremlin-Server processes" >&2
    echo " stop:   kill running Cassandra, ES, and Gremlin-Server processes" >&2
    echo " status: print Cassandra, ES, and Gremlin-Server process status" >&2
    echo " clean:  permanently delete all graph data (run when stopped)" >&2
    echo "Options:" >&2
    echo " -v      enable logging to console in addition to logfiles" >&2
}

find_verb() {
    if [ "$1" = 'start' -o \
         "$1" = 'stop' -o \
         "$1" = 'clean' -o \
         "$1" = 'status' ]; then
        COMMAND="$1"
        return 0
    fi
    return 1
}

while [ 1 ]; do
    if find_verb ${!OPTIND}; then
        OPTIND=$(($OPTIND + 1))
    elif getopts 'c:v' option; then
        case $option in
        c) GSRV_CONFIG_TAG="${OPTARG}";;
        v) VERBOSE=yes;;
        *) usage; exit 1;;
        esac
    else
        break
    fi
done

if [ -n "$COMMAND" ]; then
    $COMMAND
else
    usage
    exit 1
fi
