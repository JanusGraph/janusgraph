#!/bin/bash

# Find Hadoop
if [ "$HADOOP_HOME" = "" ] ; then
    HADOOP="hadoop"
else
    HADOOP="$HADOOP_HOME/bin/hadoop"
fi

# Read command line parameters
input=$1
output=$2
overwrite=$3
script=$4

# Deploy job to Hadoop cluster
$HADOOP jar target/faunus-0.1-SNAPSHOT-job.jar com.thinkaurelius.faunus.io.graph.FaunusGraph $1 $2 $3 $4

exit $?