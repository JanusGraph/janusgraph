#!/bin/bash

set -e
set -u

CP=/../conf
CP=$CP:$( echo `dirname $0`/../lib/*.jar . | sed 's/ /:/g')
CP=$CP:$(find -L `dirname $0`/../ext/ -name "*.jar" | tr '\n' ':')

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
MAIN_CLASS=org.janusgraph.tinkerpop.gremlin.Console
while getopts "elv" opt; do
    case "$opt" in
    e) MAIN_CLASS=org.janusgraph.tinkerpop.gremlin.ScriptExecutor
       # Stop processing gremlin.sh arguments as soon as the -e switch 
       # is seen; everything following -e becomes arguments to the 
       # ScriptExecutor main class. This maintains compatibility with
       # older deployments.
       shift $(( $OPTIND - 1 ))
       break;;
    l) eval GREMLIN_LOG_LEVEL=\$$OPTIND
       OPTIND="$(( $OPTIND + 1 ))"
       if [ "$GREMLIN_LOG_LEVEL" = "TRACE" -o \
            "$GREMLIN_LOG_LEVEL" = "DEBUG" ]; then
	   SCRIPT_DEBUG=y
       fi
       ;;
    v) MAIN_CLASS=org.apache.tinkerpop.gremlin.util.Gremlin
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
