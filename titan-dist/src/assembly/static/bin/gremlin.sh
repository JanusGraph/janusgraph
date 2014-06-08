#!/bin/bash

set -e
set -u

CP=`dirname $0`/../conf
CP=$CP:$(find -L `dirname $0`/../lib/ -name '*.jar' | tr '\n' ':')
CP=$CP:$(find -L `dirname $0`/../ext/ -name '*.jar' | tr '\n' ':')

# Check some Hadoop-related environment variables
if [ -n "${HADOOP_PREFIX:-}" ]; then
    CP="$CP:$HADOOP_PREFIX"/conf
elif [ -n "${HADOOP_CONF_DIR:-}" ]; then
    CP="$CP:$HADOOP_CONF_DIR"
elif [ -n "${HADOOP_CONF:-}" ]; then
    CP="$CP:$HADOOP_CONF"
elif [ -n "${HADOOP_HOME:-}" ]; then
    CP="$CP:$HADOOP_HOME"/conf
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
MAIN_CLASS=com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Console

while getopts "eilv" opt; do
    case "$opt" in
    e) MAIN_CLASS=com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.ScriptExecutor
       # For compatibility with behavior pre-Titan-0.5.0, stop
       # processing gremlin.sh arguments as soon as the -e switch is
       # seen; everything following -e becomes arguments to the
       # ScriptExecutor main class
       shift $(( $OPTIND - 1 ))
       break;;
    i) MAIN_CLASS=com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.InlineScriptExecutor
       # This class was brought in with Faunus/titan-hadoop. Like -e,
       # everything after this option is treated as an argument to the
       # main class.
       shift $(( $OPTIND - 1 ))
       break;;
    l) eval GREMLIN_LOG_LEVEL=\$$OPTIND
       OPTIND="$(( $OPTIND + 1 ))"
       if [ "$GREMLIN_LOG_LEVEL" = "TRACE" -o \
            "$GREMLIN_LOG_LEVEL" = "DEBUG" ]; then
	   SCRIPT_DEBUG=y
       fi
       ;;
    v) MAIN_CLASS=com.tinkerpop.gremlin.Version
    esac
done

if [ -z "${JAVA_OPTIONS:-}" ]; then
    JAVA_OPTIONS="-Dlog4j.configuration=log4j-gremlin.properties -Dgremlin.log4j.level=$GREMLIN_LOG_LEVEL"
fi

if [ -n "$SCRIPT_DEBUG" ]; then
    echo "CLASSPATH: $CLASSPATH"
    set -x
fi

# Start the JVM
$JAVA $JAVA_OPTIONS $MAIN_CLASS "$@"
