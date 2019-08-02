JanusGraph with TinkerPop’s Hadoop-Gremlin
==========================================

This chapter describes how to leverage [Apache Hadoop](https://hadoop.apache.org/) 
and [Apache Spark](https://spark.apache.org/) to configure JanusGraph for
distributed graph processing. These steps will provide an overview on
how to get started with those projects, but please refer to those
project communities to become more deeply familiar with them.

JanusGraph-Hadoop works with TinkerPop’s
[hadoop-gremlin](https://tinkerpop.apache.org/docs/{{ tinkerpop_version }}/reference/#hadoop-gremlin)
package for general-purpose OLAP.

For the scope of the example below, Apache Spark is the computing
framework and Apache Cassandra is the storage backend. The directions
can be followed with other packages with minor changes to the
configuration properties.

!!! note
    The examples in this chapter are based on running Spark in local mode
    or standalone cluster mode. Additional configuration is required when
    using Spark on YARN or Mesos.

Configuring Hadoop for Running OLAP
-----------------------------------

For running OLAP queries from the Gremlin Console, a few prerequisites
need to be fulfilled. You will need to add the Hadoop configuration
directory into the `CLASSPATH`, and the configuration directory needs to
point to a live Hadoop cluster.

Hadoop provides a distributed access-controlled file system. The Hadoop
file system is used by Spark workers running on different machines to
have a common source for file based operations. The intermediate
computations of various OLAP queries may be persisted on the Hadoop file
system.

For configuring a single node Hadoop cluster, please refer to official
[Apache Hadoop Docs](https://hadoop.apache.org/docs/r{{hadoop2_version }}/hadoop-project-dist/hadoop-common/SingleCluster.html)

Once you have a Hadoop cluster up and running, we will need to specify
the Hadoop configuration files in the `CLASSPATH`. The below document
expects that you have those configuration files located under
`/etc/hadoop/conf`.

Once verified, follow the below steps to add the Hadoop configuration to
the `CLASSPATH` and start the Gremlin Console, which will play the role
of the Spark driver program.
```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export CLASSPATH=$HADOOP_CONF_DIR
bin/gremlin.sh
```

Once the path to Hadoop configuration has been added to the `CLASSPATH`,
we can verify whether the Gremlin Console can access the Hadoop cluster
by following these quick steps:
```groovy
gremlin> hdfs
==>storage[org.apache.hadoop.fs.LocalFileSystem@65bb9029] // BAD

gremlin> hdfs
==>storage[DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_1229457199_1, ugi=user (auth:SIMPLE)]]] // GOOD
```

OLAP Traversals
---------------

JanusGraph-Hadoop works with TinkerPop’s hadoop-gremlin package for
general-purpose OLAP to traverse over the graph, and parallelize queries
by leveraging Apache Spark.

### OLAP Traversals with Spark Local

The backend demonstrated here is Cassandra for the OLAP example below.
Additional configuration will be needed that is specific to that storage
backend. The configuration is specified by the
`gremlin.hadoop.graphReader` property which specifies the class to read
data from the storage backend.

JanusGraph currently supports following graphReader classes:

* `Cassandra3InputFormat` for use with Cassandra 3
* `CassandraInputFormat` for use with Cassandra 2
* `HBaseInputFormat` and `HBaseSnapshotInputFormat` for use with HBase

The following properties file can be used to connect a JanusGraph
instance in Cassandra such that it can be used with HadoopGraph to run
OLAP queries.

```conf
# read-cassandra-3.properties
#
# Hadoop Graph Configuration
#
gremlin.graph=org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph
gremlin.hadoop.graphReader=org.janusgraph.hadoop.formats.cassandra.Cassandra3InputFormat
gremlin.hadoop.graphWriter=org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat

gremlin.hadoop.jarsInDistributedCache=true
gremlin.hadoop.inputLocation=none
gremlin.hadoop.outputLocation=output
gremlin.spark.persistContext=true

#
# JanusGraph Cassandra InputFormat configuration
#
# These properties defines the connection properties which were used while write data to JanusGraph.
janusgraphmr.ioformat.conf.storage.backend=cassandra
# This specifies the hostname & port for Cassandra data store.
janusgraphmr.ioformat.conf.storage.hostname=127.0.0.1
janusgraphmr.ioformat.conf.storage.port=9160
# This specifies the keyspace where data is stored.
janusgraphmr.ioformat.conf.storage.cassandra.keyspace=janusgraph
# This defines the indexing backned configuration used while writing data to JanusGraph.
janusgraphmr.ioformat.conf.index.search.backend=elasticsearch
janusgraphmr.ioformat.conf.index.search.hostname=127.0.0.1
# Use the appropriate properties for the backend when using a different storage backend (HBase) or indexing backend (Solr).

#
# Apache Cassandra InputFormat configuration
#
cassandra.input.partitioner.class=org.apache.cassandra.dht.Murmur3Partitioner

#
# SparkGraphComputer Configuration
#
spark.master=local[*]
spark.executor.memory=1g
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator=org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoRegistrator
```

First create a properties file with above configurations, and load the
same on the Gremlin Console to run OLAP queries as follows:
```bash
bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
plugin activated: janusgraph.imports
gremlin> :plugin use tinkerpop.hadoop
==>tinkerpop.hadoop activated
gremlin> :plugin use tinkerpop.spark
==>tinkerpop.spark activated
gremlin> // 1. Open a the graph for OLAP processing reading in from Cassandra 3
gremlin> graph = GraphFactory.open('conf/hadoop-graph/read-cassandra-3.properties')
==>hadoopgraph[cassandra3inputformat->gryooutputformat]
gremlin> // 2. Configure the traversal to run with Spark
gremlin> g = graph.traversal().withComputer(SparkGraphComputer)
==>graphtraversalsource[hadoopgraph[cassandra3inputformat->gryooutputformat], sparkgraphcomputer]
gremlin> // 3. Run some OLAP traversals
gremlin> g.V().count()
......
==>808
gremlin> g.E().count()
......
==> 8046
```

### OLAP Traversals with Spark Standalone Cluster

The steps followed in the previous section can also be used with a Spark
standalone cluster with only minor changes:

-   Update the `spark.master` property to point to the Spark master URL
    instead of local

-   Update the `spark.executor.extraClassPath` to enable the Spark
    executor to find the JanusGraph dependency jars

-   Copy the JanusGraph dependency jars into the location specified in
    the previous step on each Spark executor machine

!!! note
    We have copied all the jars under **janusgraph-distribution/lib** into
    /opt/lib/janusgraph/ and the same directory structure is created
    across all workers, and jars are manually copied across all workers.

The final properties file used for OLAP traversal is as follows:
```conf
# read-cassandra-3.properties
#
# Hadoop Graph Configuration
#
gremlin.graph=org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph
gremlin.hadoop.graphReader=org.janusgraph.hadoop.formats.cassandra.Cassandra3InputFormat
gremlin.hadoop.graphWriter=org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat

gremlin.hadoop.jarsInDistributedCache=true
gremlin.hadoop.inputLocation=none
gremlin.hadoop.outputLocation=output
gremlin.spark.persistContext=true

#
# JanusGraph Cassandra InputFormat configuration
#
# These properties defines the connection properties which were used while write data to JanusGraph.
janusgraphmr.ioformat.conf.storage.backend=cassandra
# This specifies the hostname & port for Cassandra data store.
janusgraphmr.ioformat.conf.storage.hostname=127.0.0.1
janusgraphmr.ioformat.conf.storage.port=9160
# This specifies the keyspace where data is stored.
janusgraphmr.ioformat.conf.storage.cassandra.keyspace=janusgraph
# This defines the indexing backned configuration used while writing data to JanusGraph.
janusgraphmr.ioformat.conf.index.search.backend=elasticsearch
janusgraphmr.ioformat.conf.index.search.hostname=127.0.0.1
# Use the appropriate properties for the backend when using a different storage backend (HBase) or indexing backend (Solr).

#
# Apache Cassandra InputFormat configuration
#
cassandra.input.partitioner.class=org.apache.cassandra.dht.Murmur3Partitioner

#
# SparkGraphComputer Configuration
#
spark.master=spark://127.0.0.1:7077
spark.executor.memory=1g
spark.executor.extraClassPath=/opt/lib/janusgraph/*
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator=org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoRegistrator
```

Then use the properties file as follows from the Gremlin Console:
```bash
bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
plugin activated: janusgraph.imports
gremlin> :plugin use tinkerpop.hadoop
==>tinkerpop.hadoop activated
gremlin> :plugin use tinkerpop.spark
==>tinkerpop.spark activated
gremlin> // 1. Open a the graph for OLAP processing reading in from Cassandra 3
gremlin> graph = GraphFactory.open('conf/hadoop-graph/read-cassandra-3.properties')
==>hadoopgraph[cassandra3inputformat->gryooutputformat]
gremlin> // 2. Configure the traversal to run with Spark
gremlin> g = graph.traversal().withComputer(SparkGraphComputer)
==>graphtraversalsource[hadoopgraph[cassandra3inputformat->gryooutputformat], sparkgraphcomputer]
gremlin> // 3. Run some OLAP traversals
gremlin> g.V().count()
......
==>808
gremlin> g.E().count()
......
==> 8046
```

Other Vertex Programs
---------------------

Apache TinkerPop provides various vertex programs. A vertex program runs
on each vertex until either a termination criteria is attained or a
fixed number of iterations has been reached. Due to the parallel nature
of vertex programs, they can leverage parallel computing frameworks like
Spark or Giraph to improve their performance.

Once you are familiar with how to configure JanusGraph to work with
Spark, you can run all the other vertex programs provided by Apache
TinkerPop, like Page Rank, Bulk Loading and Peer Pressure. See the
[TinkerPop VertexProgram docs](https://tinkerpop.apache.org/docs/{{ tinkerpop_version }}/reference/#vertexprogram)
for more details.
