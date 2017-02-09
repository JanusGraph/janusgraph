#!/bin/bash

set -u
set -e

if [ -z "${1:-}" ]; then
    echo "Usage: $0 directory" >&2
    echo "  Copies JanusGraph's jars to the specified directory"
    echo "  The directory is created if it does not exist"
    exit 1
fi

. "`dirname $0`"/../etc/config.sh

[ ! -e "$1" ] && mkdir -p "$1"

cd "$1"
absolutepath="`pwd`"
cd - >/dev/null

cd "`dirname $0`"/../../janusgraph-dist

mvn dependency:copy-dependencies $MVN_OPTS \
    -Pjanusgraph-release \
    -DexcludeClassifiers=tests,test,javadoc \
    -DincludeScope=runtime \
    -DoutputDirectory="$absolutepath"
