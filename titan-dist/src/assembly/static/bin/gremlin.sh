#!/bin/bash

case `uname` in
  CYGWIN*)
    CP=`dirname $0`/../conf
    CP=$CP;$( echo `dirname $0`/../lib/*.jar . | sed 's/ /;/g')
    CP=$CP;$(find -L `dirname $0`/../ext/ -name "*.jar" | tr '\n' ';')
    ;;
  *)
    CP=`dirname $0`/../conf
    CP=$CP:$( echo `dirname $0`/../lib/*.jar . | sed 's/ /:/g')
    CP=$CP:$(find -L `dirname $0`/../ext/ -name "*.jar" | tr '\n' ':')
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
if [ "$1" = "-e" ]; then
  k=$2
  if [ $# -gt 2 ]; then
    for (( i=3 ; i < $# + 1 ; i++ ))
    do
      eval a=\$$i
      k="$k \"$a\""
    done
  fi

  eval $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.titan.tinkerpop.gremlin.ScriptExecutor $k
else
  if [ "$1" = "-v" ]; then
    $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.tinkerpop.gremlin.Version
  else
    $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.titan.tinkerpop.gremlin.Console $@
  fi
fi

# Return the program's exit code
exit $?
