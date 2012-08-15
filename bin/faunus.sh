#!/bin/bash

# Find Hadoop
if [ "$HADOOP_HOME" = "" ] ; then
    HADOOP="hadoop"
else
    HADOOP="$HADOOP_HOME/bin/hadoop"
fi

# Deploy job to Hadoop cluster
$HADOOP jar target/faunus-*-job.jar com.thinkaurelius.faunus.FaunusGraph ${1+"$@"}

exit $?