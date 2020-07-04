# JanusGraph with TinkerPop’s Hadoop-Gremlin

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

## Configuring Hadoop for Running OLAP

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

## OLAP Traversals

JanusGraph-Hadoop works with TinkerPop’s hadoop-gremlin package for
general-purpose OLAP to traverse over the graph, and parallelize queries
by leveraging Apache Spark.

### OLAP Traversals with Spark Local

OLAP Examples below are showing configuration examples for directly supported 
backends by JanusGraph. Additional configuration will be needed that 
is specific to that storage backend. The configuration is specified by the
`gremlin.hadoop.graphReader` property which specifies the class to read
data from the storage backend.

JanusGraph directly supports following graphReader classes:

* `CqlInputFormat` for use with Cassandra
* `HBaseInputFormat` and `HBaseSnapshotInputFormat` for use with HBase

The following `.properties` files can be used to connect a JanusGraph
instance such that it can be used with HadoopGraph to run OLAP queries.

```properties tab='read-cql.properties'
{!../janusgraph-dist/src/assembly/static/conf/hadoop-graph/read-cql.properties!}
```

```properties tab='read-hbase.properties'
{!../janusgraph-dist/src/assembly/static/conf/hadoop-graph/read-hbase.properties!}
```

First create a properties file with above configurations, and load the
same on the Gremlin Console to run OLAP queries as follows:

=== "read-cql.properties"
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
    gremlin> graph = GraphFactory.open('conf/hadoop-graph/read-cql.properties')
    ==>hadoopgraph[cqlinputformat->gryooutputformat]
    gremlin> // 2. Configure the traversal to run with Spark
    gremlin> g = graph.traversal().withComputer(SparkGraphComputer)
    ==>graphtraversalsource[hadoopgraph[cqlinputformat->gryooutputformat], sparkgraphcomputer]
    gremlin> // 3. Run some OLAP traversals
    gremlin> g.V().count()
    ......
    ==>808
    gremlin> g.E().count()
    ......
    ==> 8046
    ```

=== "read-hbase.properties"
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
    gremlin> // 1. Open a the graph for OLAP processing reading in from HBase
    gremlin> graph = GraphFactory.open('conf/hadoop-graph/read-hbase.properties')
    ==>hadoopgraph[hbaseinputformat->gryooutputformat]
    gremlin> // 2. Configure the traversal to run with Spark
    gremlin> g = graph.traversal().withComputer(SparkGraphComputer)
    ==>graphtraversalsource[hadoopgraph[hbaseinputformat->gryooutputformat], sparkgraphcomputer]
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

```properties tab='read-cql-standalone-cluster.properties'
{!../janusgraph-dist/src/assembly/static/conf/hadoop-graph/read-cql-standalone-cluster.properties!}
```

```properties tab='read-hbase-standalone-cluster.properties'
{!../janusgraph-dist/src/assembly/static/conf/hadoop-graph/read-hbase-standalone-cluster.properties!}
```

Then use the properties file as follows from the Gremlin Console:

=== "read-cql-standalone-cluster.properties"
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
    gremlin> graph = GraphFactory.open('conf/hadoop-graph/read-cql-standalone-cluster.properties')
    ==>hadoopgraph[cqlinputformat->gryooutputformat]
    gremlin> // 2. Configure the traversal to run with Spark
    gremlin> g = graph.traversal().withComputer(SparkGraphComputer)
    ==>graphtraversalsource[hadoopgraph[cqlinputformat->gryooutputformat], sparkgraphcomputer]
    gremlin> // 3. Run some OLAP traversals
    gremlin> g.V().count()
    ......
    ==>808
    gremlin> g.E().count()
    ......
    ==> 8046
    ```

=== "read-hbase-standalone-cluster.properties"
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
    gremlin> // 1. Open a the graph for OLAP processing reading in from HBase
    gremlin> graph = GraphFactory.open('conf/hadoop-graph/read-hbase-standalone-cluster.properties')
    ==>hadoopgraph[hbaseinputformat->gryooutputformat]
    gremlin> // 2. Configure the traversal to run with Spark
    gremlin> g = graph.traversal().withComputer(SparkGraphComputer)
    ==>graphtraversalsource[hadoopgraph[hbaseinputformat->gryooutputformat], sparkgraphcomputer]
    gremlin> // 3. Run some OLAP traversals
    gremlin> g.V().count()
    ......
    ==>808
    gremlin> g.E().count()
    ......
    ==> 8046
    ```

## Other Vertex Programs

Apache TinkerPop provides various vertex programs. A vertex program runs
on each vertex until either a termination criteria is attained or a
fixed number of iterations has been reached. Due to the parallel nature
of vertex programs, they can leverage parallel computing framework like
Spark to improve their performance.

Once you are familiar with how to configure JanusGraph to work with
Spark, you can run all the other vertex programs provided by Apache
TinkerPop, like Page Rank, Bulk Loading and Peer Pressure. See the
[TinkerPop VertexProgram docs](https://tinkerpop.apache.org/docs/{{ tinkerpop_version }}/reference/#vertexprogram)
for more details.
