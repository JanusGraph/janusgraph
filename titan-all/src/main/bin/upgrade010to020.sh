#!/bin/bash

case `uname` in
  CYGWIN*)
    CP=$( echo `dirname $0`/../lib/*.jar . | sed 's/ /;/g')
    ;;
  *)
    CP=$( echo `dirname $0`/../lib/*.jar . | sed 's/ /:/g')
esac
#echo $CP

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