#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e
set -u

USER_DIR=`pwd`

cd $(dirname $0)
DIR=`pwd`

SCRIPT_NAME=`basename $0`
SOURCE="${SCRIPT_NAME}"
while [ -h "${SCRIPT_NAME}" ]; do
  SOURCE="$(readlink "${SCRIPT_NAME}")"
  DIR="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
  cd ${DIR}
done
BIN="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
# Set $CFG to $BIN/../conf
cd -P $BIN/../conf
CFG=$(pwd)
# Set $LIB to $BIN/../lib
cd -P $BIN/../lib
LIB=$(pwd)
# Set $LIB to $BIN/../ext
cd -P $BIN/../ext
EXT=$(pwd)
# Initialize classpath to $CFG
CP="$CFG"
# Add the slf4j-log4j12 binding
CP="$CP":$(find -L $LIB -name 'slf4j-log4j12*.jar' | sort | tr '\n' ':')
# Add the jars in $BIN/../lib that start with "janusgraph"
CP="$CP":$(find -L $LIB -name 'janusgraph*.jar' | sort | tr '\n' ':')
# Add the remaining jars in $BIN/../lib.
CP="$CP":$(find -L $LIB -name '*.jar' \
                \! -name 'janusgraph*' \
                \! -name 'slf4j-log4j12*.jar' | sort | tr '\n' ':')
# Add the jars in $BIN/../ext (at any subdirectory depth)
CP="$CP":$(find -L $EXT -name '*.jar' | sort | tr '\n' ':')

cd ..
SYSTEM_EXT_DIR="`pwd`/ext"

JAVA_OPTIONS=${JAVA_OPTIONS:-}

if [ ! -z "${JAVA_OPTIONS}" ]; then
  USER_EXT_DIR=$(grep -o '\-Dtinkerpop.ext=\(\([^"][^ ]*\)\|\("[^"]*"\)\)' <<< "${JAVA_OPTIONS}" | cut -f2 -d '=' | xargs -0 echo)
  if [ ! -z "${USER_EXT_DIR}" -a ! -d "${USER_EXT_DIR}" ]; then
    mkdir -p "${USER_EXT_DIR}"
    cp -R ${SYSTEM_EXT_DIR}/* ${USER_EXT_DIR}/
  fi
fi

case `uname` in
  CYGWIN*)
    CP="$(cygpath -p -w "$CP")"
    ;;
esac

CP=$CP:$( find -L "${SYSTEM_EXT_DIR}" "${USER_EXT_DIR:-${SYSTEM_EXT_DIR}}" -mindepth 1 -maxdepth 1 -type d | \
          sort -u | sed 's/$/\/plugin\/*/' | tr '\n' ':' )

export CLASSPATH="${CLASSPATH:-}:$CP"

# Find Java
if [ -z "${JAVA_HOME:-}" ]; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set default message threshold for Log4j Gremlin's console appender
if [ -z "${GREMLIN_LOG_LEVEL:-}" ]; then
    GREMLIN_LOG_LEVEL=WARN
fi

# Script debugging is disabled by default, but can be enabled with -l
# TRACE or -l DEBUG or enabled by exporting
# SCRIPT_DEBUG=nonemptystring to gremlin.sh's environment
if [ -z "${SCRIPT_DEBUG:-}" ]; then
    SCRIPT_DEBUG=
fi

# Process options
MAIN_CLASS=org.apache.tinkerpop.gremlin.console.Console
while getopts ":lv" opt; do
    case "$opt" in
    l) eval GREMLIN_LOG_LEVEL=\$$OPTIND
       OPTIND="$(( $OPTIND + 1 ))"
       if [ "$GREMLIN_LOG_LEVEL" = "TRACE" -o \
            "$GREMLIN_LOG_LEVEL" = "DEBUG" ]; then
	   SCRIPT_DEBUG=y
       fi
       ;;
    v) MAIN_CLASS=org.janusgraph.core.JanusGraph
    esac
done

if [ -z "${HADOOP_GREMLIN_LIBS:-}" ]; then
    export HADOOP_GREMLIN_LIBS="$LIB"
fi

JAVA_OPTIONS="${JAVA_OPTIONS} -Duser.working_dir=${USER_DIR} -Dtinkerpop.ext=${USER_EXT_DIR:-${SYSTEM_EXT_DIR}} -Dlog4j.configuration=conf/log4j-console.properties -Dgremlin.log4j.level=$GREMLIN_LOG_LEVEL -javaagent:$LIB/jamm-0.3.0.jar -Dgremlin.io.kryoShimService=org.janusgraph.hadoop.serialize.JanusGraphKryoShimService"
JAVA_OPTIONS=$(awk -v RS=' ' '!/^$/ {if (!x[$0]++) print}' <<< "${JAVA_OPTIONS}" | grep -v '^$' | paste -sd ' ' -)

if [ -n "$SCRIPT_DEBUG" ]; then
    # in debug mode enable debugging of :install command
    JAVA_OPTIONS="${JAVA_OPTIONS} -Divy.message.logger.level=4 -Dgroovy.grape.report.downloads=true"
    echo "CLASSPATH: $CLASSPATH"
    set -x
fi

# Start the JVM, execute the application, and return its exit code
exec $JAVA $JAVA_OPTIONS $MAIN_CLASS "$@"
