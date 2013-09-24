#!/bin/bash
BIN="`dirname $0`"

start() {
    echo "Starting Cassandra..." >&2
    "$BIN"/cassandra || exit 1
    echo "Starting Titan + Rexster..." >&2
    "$BIN"/rexster.sh -d -s || exit 2
    echo "Cassandra + Rexster + Titan stack started." >&2
    echo "Run $BIN/rexster-console.sh to connect."    >&2
}

stop() {
    echo "Stopping Titan + Rexster..." >&2
    pkill -f com.tinkerpop.rexster.Application 
    echo "Stopping Cassandra..." >&2
    pkill -f cassandra
}

clean() {
    cd "$BIN"/../db || exit 1
    rm -rf cassandra
}

usage() {
    echo "Usage: $0 {start|stop|clean}" >&2
    echo " start: fork Cassandra and Rexster+Titan processes" >&2
    echo " stop:  kill running Cassandra and Rexster+Titan processes" >&2
    echo " clean: permanently delete all graph data (run when stopped)" >&2
}

case $1 in
    start) start;;
    stop)  stop;;
    clean) clean;;
    *)     usage;;
esac
