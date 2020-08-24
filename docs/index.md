## The Benefits of JanusGraph

JanusGraph is designed to support the processing of graphs so large that
they require storage and computational capacities beyond what a single
machine can provide. Scaling graph data processing for real time
traversals and analytical queries is JanusGraph’s foundational benefit.
This section will discuss the various specific benefits of JanusGraph
and its underlying, supported persistence solutions.

### General JanusGraph Benefits

- Support for very large graphs. JanusGraph graphs scale with the
    number of machines in the cluster.
- Support for very many concurrent transactions and operational graph
    processing. JanusGraph’s transactional capacity scales with the
    number of machines in the cluster and answers complex traversal
    queries on huge graphs in milliseconds.
- Support for global graph analytics and batch graph processing
    through the Hadoop framework.
- Support for geo, numeric range, and full text search for vertices
    and edges on very large graphs.
- Native support for the popular property graph data model exposed by
    [Apache TinkerPop](https://tinkerpop.apache.org/).
- Native support for the graph traversal language
    [Gremlin](https://tinkerpop.apache.org/gremlin.html).
  - Numerous graph-level configurations provide knobs for tuning
    performance.
- Vertex-centric indices provide vertex-level querying to alleviate
    issues with the infamous [super node problem](https://www.datastax.com/blog/2012/10/solution-supernode-problem).
- Provides an optimized disk representation to allow for efficient use
    of storage and speed of access.
- Open source under the liberal [Apache 2 license](https://en.wikipedia.org/wiki/Apache_License).

### Benefits of JanusGraph with Apache Cassandra

<div style="float: right;">
    <img src="cassandra-small.svg">
</div>

-   [Continuously available](https://en.wikipedia.org/wiki/Continuous_availability)
    with no single point of failure.
-   No read/write bottlenecks to the graph as there is no master/slave
    architecture.

-   [Elastic scalability](https://en.wikipedia.org/wiki/Elastic_computing) allows
    for the introduction and removal of machines.
-   Caching layer ensures that continuously accessed data is available
    in memory.
-   Increase the size of the cache by adding more machines to the
    cluster.
-   Integration with [Apache Hadoop](https://hadoop.apache.org/).
-   Open source under the liberal Apache 2 license.

### Benefits of JanusGraph with HBase

<div style="float: right;">
    <img src="https://hbase.apache.org/images/hbase_logo.png">
</div>

-   Tight integration with the [Apache Hadoop](https://hadoop.apache.org/) ecosystem.
-   Native support for [strong consistency](https://en.wikipedia.org/wiki/Strong_consistency).
-   Linear scalability with the addition of more machines.
-   [Strictly consistent](https://en.wikipedia.org/wiki/Strict_consistency) reads and writes.
-   Convenient base classes for backing Hadoop
    [MapReduce](https://en.wikipedia.org/wiki/MapReduce) jobs with HBase
    tables.
-   Support for exporting metrics via
    [JMX](https://en.wikipedia.org/wiki/Java_Management_Extensions).
-   Open source under the liberal Apache 2 license.

### JanusGraph and the CAP Theorem

> Despite your best efforts, your system will experience enough faults
> that it will have to make a choice between reducing yield (i.e., stop
> answering requests) and reducing harvest (i.e., giving answers based
> on incomplete data). This decision should be based on business
> requirements.
>
> —  [Coda Hale](https://codahale.com/you-cant-sacrifice-partition-tolerance)

When using a database, the [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem) should be thoroughly
considered (C=Consistency, A=Availability, P=Partitionability).
JanusGraph is distributed with 3 supporting backends: [Apache Cassandra](https://cassandra.apache.org/),
 [Apache HBase](https://hbase.apache.org/), and [Oracle Berkeley DB Java Edition](https://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html).
Note that BerkeleyDB JE is a non-distributed database and is typically
only used with JanusGraph for testing and exploration purposes.

HBase gives preference to consistency at the expense of yield, i.e. the
probability of completing a request. Cassandra gives preference to
availability at the expense of harvest, i.e. the completeness of the
answer to the query (data available/complete data).
