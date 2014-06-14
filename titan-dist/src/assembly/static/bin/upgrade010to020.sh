#!/bin/bash

# Returns the absolute path of this script regardless of symlinks
abs_path() {
    # From: http://stackoverflow.com/a/246128
    #   - To resolve finding the directory after symlinks
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

case `uname` in
  CYGWIN*)
    CP=$( echo `abs_path`/../lib/*.jar . | sed 's/ /;/g')
    CP=$CP:$(find -L `abs_path`/../ext/ -name "*.jar" | tr '\n' ';')
    ;;
  *)
    CP=$( echo `abs_path`/../lib/*.jar . | sed 's/ /:/g')
    CP=$CP:$(find -L `abs_path`/../ext/ -name "*.jar" | tr '\n' ':')
esac

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS="-Xms32m -Xmx512m"
fi

# Launch the application
$JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.titan.upgrade.Upgrade010to020 $@
