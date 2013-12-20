#!/bin/bash

BIN="`dirname $0`"
# Rexster doesn't allow GraphConfiguration implementations a clean way
# to know the path to the rexster.xml config file.  This makes implementing
# Titan's relative storage directory interpretation (relative to the file in
# which they appear rather than the process's current working directory)
# impossible.  Change to a fixed working directory as a workaround.  Rexster
# XML configs can assume this working directory.
cd "$BIN/../rexhome"

# This script is tested mostly under Linux and Mac OS X, but it 
# can also run through Cygwin.
#
# When running through Cygwin, Java file paths and CLASSPATH require
# special handling.  Cygwin uses *NIX style paths, but Java is outside
# Cygwin's control and uses Windows style paths.  Any CLASSPATH or 
# file paths strings provided to Java must be sent through the utility
# command `cygpath --path --windows`.

set_unix_paths() {
	CP="$(echo ../conf ../lib/*.jar . | tr ' ' ':')"
	CP="$CP:$(find -L ../ext/ -name "*.jar" | tr '\n' ':')"
	export CLASSPATH="$CP"
	PUBLIC=../public/
	LOG_DIR=../log
}

convert_unix_paths_to_win_paths() {
	export CLASSPATH="$(echo $CLASSPATH | cygpath --windows --path -f -)"
	PUBLIC="$(echo $PUBLIC | cygpath --windows --path -f -)"
	LOG_DIR="$(echo $LOG_DIR | cygpath --windows --path -f -)"
}

set_unix_paths
case "`uname`" in
    CYGWIN*) convert_unix_paths_to_win_paths ;;
esac

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS="-server -Xms128m -Xmx512m -Dtitan.logdir=$LOG_DIR"
fi

# Let Cassandra have 7199
JAVA_OPTIONS="$JAVA_OPTIONS \
              -Dcom.sun.management.jmxremote.port=7299 \
              -Dcom.sun.management.jmxremote.ssl=false \
              -Dcom.sun.management.jmxremote.authenticate=false"

# Launch the application
"$JAVA" $JAVA_OPTIONS com.tinkerpop.rexster.Application "$@"
# Return the program's exit code
exit $?
