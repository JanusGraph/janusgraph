#!/bin/bash

set_unix_paths() {
    # From: http://stackoverflow.com/a/246128
    #   - To resolve finding the directory after symlinks
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    BIN="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
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
