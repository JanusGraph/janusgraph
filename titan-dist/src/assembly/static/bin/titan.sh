#!/bin/bash

BIN="`dirname $0`"
REXSTER_CONFIG_TAG=cassandra-es
: ${CASSANDRA_STARTUP_TIMEOUT_S:=60}
VERBOSE=
COMMAND=

wait_for_cassandra() {
    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $CASSANDRA_STARTUP_TIMEOUT_S ))
    local status_thrift=

    while [ $now_s -le $stop_s ]; do
        # The \r\n deletion bit is necessary for Cygwin compatibility
        status_thrift="`$BIN/nodetool statusthrift 2>/dev/null | tr -d '\n\r'`"
        if [ $? -eq 0 -a 'running' = "$status_thrift" ]; then
            echo 'Started Cassandra.  Thrift service is alive.'
            return 0
        fi
        sleep 2
        now_s=`date '+%s'`
    done

    echo "Cassandra startup timeout exceeded ($CASSANDRA_STARTUP_TIMEOUT_S seconds)" >&2
    return 1
}

start() {
    echo "Starting Cassandra..." >&2
    if [ -n "$VERBOSE" ]; then
        CASSANDRA_INCLUDE="$BIN"/cassandra.in.sh "$BIN"/cassandra || exit 1
    else
        CASSANDRA_INCLUDE="$BIN"/cassandra.in.sh "$BIN"/cassandra >/dev/null 2>&1 || exit 1
    fi
    wait_for_cassandra || {
        echo 'Failed to start Cassandra or starting Cassandra timed out.' >&2
        echo "See $BIN/../log/cassandra.log for Cassandra log output."    >&2
        return 1
    }
    echo "Forking Titan + Rexster..." >&2
    if [ -n "$VERBOSE" ]; then
        "$BIN"/rexster.sh -s -wr public -c ../conf/rexster-${REXSTER_CONFIG_TAG}.xml &
    else
        "$BIN"/rexster.sh -s -wr public -c ../conf/rexster-${REXSTER_CONFIG_TAG}.xml >/dev/null 2>&1 &
    fi
    disown
    echo "Forked Titan + Rexster." >&2
    echo "Rexster may need a few more seconds to finish bootstrapping." >&2
    echo "Run $BIN/rexster-console.sh to connect." >&2
}

stop() {
    kill_class 'Titan + Rexster' com.tinkerpop.rexster.Application 
    kill_class Cassandra org.apache.cassandra.service.CassandraDaemon
}

kill_class() {
    local p=`jps -l | grep "$2" | awk '{print $1}'`
    if [ -z "$p" ]; then
        echo "$1 ($2) not found in the java process table"
        return
    fi
    echo "Killing $1 (pid $p)..." >&2
    if [ "`uname -o`" = 'Cygwin' ]; then
        taskkill /F /PID "$p"
    else
        kill "$p"
    fi
}

status_class() {
    local p=`jps -l | grep "$2" | awk '{print $1}'`
    if [ -n "$p" ]; then
        echo "$1 ($2) is running with pid $p"
        return 0
    else
        echo "$1 ($2) does not appear in the java process table"
        return 1
    fi
}

status() {
    status_class 'Titan + Rexster' com.tinkerpop.rexster.Application 
    status_class Cassandra org.apache.cassandra.service.CassandraDaemon
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

    if cd "$BIN"/../log; then
        rm -f cassandra.log
        rm -f rexstitan.log
        echo "Deleted logs in `pwd`" >&2
        cd - >/dev/null
    fi
}

usage() {
    echo "Usage: $0 [options] {start|stop|status|clean}" >&2
    echo " start:  fork Cassandra and Rexster+Titan processes" >&2
    echo " stop:   kill running Cassandra and Rexster+Titan processes" >&2
    echo " status: print Cassandra and Rexster+Titan process status" >&2
    echo " clean:  permanently delete all graph data (run when stopped)" >&2
    echo "Options:" >&2
    echo " -v      enable logging to console in addition to logfiles" >&2
    echo " -c str  configure rexster with conf/rexster-<str>.xml" >&2
    echo "         recognized arguments to -c:" >&2
    shopt -s nullglob
    for f in "$BIN"/../conf/rexster-*.xml; do
        f="`basename $f`"
        f="${f#rexster-}"
        f="${f%.xml}"
        echo "           $f" >&2
    done
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
        c) REXSTER_CONFIG_TAG="${OPTARG}";;
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
