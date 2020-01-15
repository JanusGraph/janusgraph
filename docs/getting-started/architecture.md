JanusGraph is a graph database engine. JanusGraph itself is focused on
compact graph serialization, rich graph data modeling, and efficient
query execution. In addition, JanusGraph utilizes Hadoop for graph
analytics and batch graph processing. JanusGraph implements robust,
modular interfaces for data persistence, data indexing, and client
access. JanusGraph’s modular architecture allows it to interoperate with
a wide range of storage, index, and client technologies; it also eases
the process of extending JanusGraph to support new ones.

Between JanusGraph and the disks sits one or more storage and indexing
adapters. JanusGraph comes standard with the following adapters, but
JanusGraph’s modular architecture supports third-party adapters.

-   Data storage:
    -   [Apache Cassandra](../storage-backend/cassandra.md)
    -   [Apache HBase](../storage-backend/hbase.md)
    -   [Oracle Berkeley DB Java Edition](../storage-backend/bdb.md)
-   Indices, which speed up and enable more complex queries:
    -   [Elasticsearch](../index-backend/elasticsearch.md)
    -   [Apache Solr](../index-backend/solr.md)
    -   [Apache Lucene](../index-backend/lucene.md)

Broadly speaking, applications can interact with JanusGraph in two ways:

-   Embed JanusGraph inside the application executing
    [Gremlin](https://tinkerpop.apache.org/docs/{{ tinkerpop_version }}/reference#graph-traversal-steps)
    queries directly against the graph within the same JVM. Query
    execution, JanusGraph’s caches, and transaction handling all happen
    in the same JVM as the application while data retrieval from the
    storage backend may be local or remote.

-   Interact with a local or remote JanusGraph instance by submitting
    Gremlin queries to the server. JanusGraph natively supports the
    Gremlin Server component of the [Apache TinkerPop](https://tinkerpop.apache.org/) stack.

![High-level JanusGraph Architecture and Context](architecture-layer-diagram.svg)
