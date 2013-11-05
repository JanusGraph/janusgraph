#!/bin/bash

BIN="`dirname $0`"
# Rexster doesn't allow GraphConfiguration implementations a clean way
# to know the path to the rexster.xml config file.  This makes implementing
# Titan's relative storage directory interpretation (relative to the file in
# which they appear rather than the process's current working directory)
# impossible.  Change to the parent of the bin directory as a workaround.
cd "$BIN/.."

CP="conf"
CP="$CP:$( echo lib/*.jar . | sed 's/ /:/g')"
CP="$CP:$(find -L ext/ -name "*.jar" | tr '\n' ':')"
export CLASSPATH="$CP"

PUBLIC='public/'
FOREGROUND=1
ARGS=

while getopts 'ds:' option; do
    case $option in
        'd') FOREGROUND=0;;
        's') ARGS="-wr $PUBLIC -c $OPTARG $ARGS"; ARGS="-$option $ARGS";;
         * ) ARGS="-$option $ARGS";;
    esac
done

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS="-Xms128m -Xmx512m -Dlog4j.configuration=log4j-rexstitan.properties -Dtitan.logdir=log"
fi

# Let Cassandra have 7199
JAVA_OPTIONS="$JAVA_OPTIONS \
              -Dcom.sun.management.jmxremote.port=7299 \
              -Dcom.sun.management.jmxremote.ssl=false \
              -Dcom.sun.management.jmxremote.authenticate=false"

# Launch the application
if [ 1 -eq $FOREGROUND ]; then
    $JAVA $JAVA_OPTIONS com.tinkerpop.rexster.Application $ARGS
    # Return the program's exit code
    exit $?
else
    $JAVA $JAVA_OPTIONS com.tinkerpop.rexster.Application $ARGS &
    disown
fi
