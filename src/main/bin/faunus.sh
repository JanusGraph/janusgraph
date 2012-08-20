#!/bin/bash

# Find Hadoop
if [ "$HADOOP_HOME" = "" ] ; then
    HADOOP="hadoop"
else
    HADOOP="$HADOOP_HOME/bin/hadoop"
fi

# Deploy job to Hadoop cluster
$HADOOP jar lib/faunus-*-job.jar com.thinkaurelius.faunus.FaunusPipeline ${1+"$@"}

exit $?