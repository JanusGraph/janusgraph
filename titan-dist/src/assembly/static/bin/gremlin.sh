#!/bin/bash

# Change to the parent directory of bin/
# Titan configs with relative paths to storage directories rely on this step
cd "`dirname $0`"/..

case `uname` in
  CYGWIN*)
    CP=conf
    CP=$CP;$( echo lib/*.jar . | sed 's/ /;/g')
    CP=$CP;$(find -L ext/ -name "*.jar" | tr '\n' ';')
    ;;
  *)
    CP=conf
    CP=$CP:$( echo lib/*.jar . | sed 's/ /:/g')
    CP=$CP:$(find -L ext/ -name "*.jar" | tr '\n' ':')
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
