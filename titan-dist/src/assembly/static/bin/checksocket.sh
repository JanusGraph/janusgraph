#!/bin/sh

cd "`dirname $0`"/..

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME"/bin/java
else
    JAVA=java
fi

"$JAVA" -cp lib/titan-core-*.jar com.thinkaurelius.titan.util.system.CheckSocket $1 $2
