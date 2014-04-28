#!/bin/bash

case `uname` in
  CYGWIN*)
    CP=$( echo `dirname $0`/../lib/*.jar . | sed 's/ /;/g')
    ;;
  *)
    CP=$( echo `dirname $0`/../lib/*.jar . | sed 's/ /:/g')
esac

# Find Hadoop
if [ "$HADOOP_PREFIX" != "" ] ; then
  CP=$CP:$HADOOP_PREFIX/conf
elif [ "$HADOOP_CONF_DIR" != "" ] ; then
  CP=$CP:$HADOOP_CONF_DIR
elif [ "$HADOOP_CONF" != "" ] ; then
  CP=$CP:$HADOOP_CONF
else
  CP=$CP:$HADOOP_HOME/conf
fi

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
if [ "$1" = "-e" -o "$1" = "-i" ]; then
  k=$2
  if [ $# -gt 2 ]; then
    for (( i=3 ; i < $# + 1 ; i++ ))
    do
      eval a=\$$i
      k="$k \"$a\""
    done
  fi

  if [ "$1" = "-e" ]; then
      eval $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.faunus.tinkerpop.gremlin.ScriptExecutor $k
  else
      eval $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.faunus.tinkerpop.gremlin.InlineScriptExecutor $k
  fi
else
  if [ "$1" = "-v" ]; then
    $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.faunus.tinkerpop.gremlin.Version
  else
    $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.faunus.tinkerpop.gremlin.Console
  fi
fi

# Return the program's exit code
exit $?