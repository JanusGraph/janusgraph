#!/bin/bash

BIN="`dirname $0`"
REXSTER_CONFIG=conf/rexster-cassandra.xml

start() {
    echo "Starting Cassandra..." >&2
    CASSANDRA_INCLUDE=`dirname $0`/cassandra.in.sh "$BIN"/cassandra || exit 1
    echo "Starting Titan + Rexster..." >&2
    sleep 5
    "$BIN"/rexster.sh -d -s $REXSTER_CONFIG || exit 2
    echo "Processes forked.  Setup may take some time." >&2
    echo "Run $BIN/rexster-console.sh to connect."      >&2
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
    kill "$p"
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
    echo -n "Are you sure you want to delete all stored data? [y/N] " >&2
    read response
    if [ "$response" != "y" -a "$response" != "Y" ]; then
        echo "Response $response did not equal \"y\" or \"Y\".  Canceling clean operation." >&2
        return 0
    fi
    cd "$BIN"/../db 2>/dev/null || return
    rm -rf cassandra es
}

usage() {
    echo "Usage: $0 {start|stop|clean}" >&2
    echo " start: fork Cassandra and Rexster+Titan processes" >&2
    echo " stop:  kill running Cassandra and Rexster+Titan processes" >&2
    echo " clean: permanently delete all graph data (run when stopped)" >&2
}

while getopts 'c:' option; do
    case $option in
    c) REXSTER_CONFIG="conf/rexster-${OPTARG}.xml"; shift;;
    *) usage; exit 1;;
    esac
    shift
done

case $1 in
    start)  start;;
    stop)   stop;;
    clean)  clean;;
    status) status;;
    *)      usage;;
esac
