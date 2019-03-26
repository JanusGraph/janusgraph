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

set -e
set -u

CP=`dirname $0`/../conf
CP=$CP:$(find -L `dirname $0`/../lib/ -name '*.jar' | tr '\n' ':')
CP=$CP:$(find -L `dirname $0`/../ext/ -name '*.jar' | tr '\n' ':')
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

CP=`abs_path`/../conf
CP=$CP:$(find -L `abs_path`/../lib/ -name '*.jar' | tr '\n' ':')
CP=$CP:$(find -L `abs_path`/../ext/ -name '*.jar' | tr '\n' ':')

# Check some Hadoop-related environment variables
if [ -n "${HADOOP_PREFIX:-}" ]; then
    # Check Hadoop 2 first
    if [ -d "$HADOOP_PREFIX"/etc/hadoop ]; then
        CP="$CP:$HADOOP_PREFIX"/etc/hadoop
    elif [ -d "$HADOOP_PREFIX"/conf ]; then
        # Then try Hadoop 1
        CP="$CP:$HADOOP_PREFIX"/conf
    fi
elif [ -n "${HADOOP_CONF_DIR:-}" ]; then
    CP="$CP:$HADOOP_CONF_DIR"
elif [ -n "${HADOOP_CONF:-}" ]; then
    CP="$CP:$HADOOP_CONF"
elif [ -n "${HADOOP_HOME:-}" ]; then
    # Check Hadoop 2 first
    if [ -d "$HADOOP_HOME"/etc/hadoop ]; then
        CP="$CP:$HADOOP_HOME"/etc/hadoop
    elif [ -d "$HADOOP_HOME"/conf ]; then
        # Then try Hadoop 1
        CP="$CP:$HADOOP_HOME"/conf
    fi
fi

# Convert from *NIX to Windows path convention if needed
case `uname` in
    CYGWIN*) CP=`cygpath -p -w "$CP"`
esac

export CLASSPATH="${CLASSPATH:-}:$CP"

# Find Java
if [ -z "${JAVA_HOME:-}" ]; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set default message threshold for Log4j Gremlin's console appender
if [ -z "${GREMLIN_LOG_LEVEL:-}" -o "${GREMLIN_MR_LOG_LEVEL:-}" ]; then
    GREMLIN_LOG_LEVEL=INFO
    GREMLIN_MR_LOG_LEVEL=INFO
fi

# Script debugging is disabled by default, but can be enabled with -l
# TRACE or -l DEBUG or enabled by exporting
# SCRIPT_DEBUG=nonemptystring to gremlin.sh's environment
if [ -z "${SCRIPT_DEBUG:-}" ]; then
    SCRIPT_DEBUG=
fi

# Process options
MAIN_CLASS=org.janusgraph.util.system.ConfigurationLint

while getopts "eilv" opt; do
    case "$opt" in
    l) eval GREMLIN_LOG_LEVEL=\$$OPTIND
       GREMLIN_MR_LOG_LEVEL="$GREMLIN_LOG_LEVEL"
       OPTIND="$(( $OPTIND + 1 ))"
       if [ "$GREMLIN_LOG_LEVEL" = "TRACE" -o \
            "$GREMLIN_LOG_LEVEL" = "DEBUG" ]; then
	   SCRIPT_DEBUG=y
       fi
       ;;
    esac
done

if [ -z "${JAVA_OPTIONS:-}" ]; then
    JAVA_OPTIONS="-Dlog4j.configuration=log4j-gremlin.properties"
    JAVA_OPTIONS="$JAVA_OPTIONS -Dgremlin.log4j.level=$GREMLIN_LOG_LEVEL"
    JAVA_OPTIONS="$JAVA_OPTIONS -Dgremlin.mr.log4j.level=$GREMLIN_MR_LOG_LEVEL"
fi

if [ -n "$SCRIPT_DEBUG" ]; then
    echo "CLASSPATH: $CLASSPATH"
    set -x
fi

# Start the JVM
$JAVA $JAVA_OPTIONS $MAIN_CLASS "$@"
