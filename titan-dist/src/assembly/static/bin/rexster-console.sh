#!/bin/bash

set_unix_paths() {
    BIN="$(dirname $0)"
    CP="$(echo $BIN/../conf $BIN/../lib/*.jar . | tr ' ' ':')"
    CP="$CP:$(find -L $BIN/../ext/ -name "*.jar" | tr '\n' ':')"
}

convert_unix_paths_to_win_paths() {
    CP="$(echo $CP | cygpath --windows --path -f -)"
}

set_unix_paths
case "`uname`" in
    CYGWIN*)
        echo "WARNING: rexster-console.sh is unsupported on Cygwin."
        echo "Use rexster-console.bat from the Windows Command Prompt instead."
    ;;
esac

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS="-server -Xms32m -Xmx512m"
fi

# Launch the application
"$JAVA" $JAVA_OPTIONS -cp $CP com.tinkerpop.rexster.console.RexsterConsole $@

# Return the program's exit code
exit $?
