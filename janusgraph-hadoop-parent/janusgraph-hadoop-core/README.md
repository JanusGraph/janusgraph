JanusGraph Hadoop module provides a [property graph](https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model) analytics engine based on [Apache Hadoop](http://hadoop.apache.org/). A [breadth-first](http://en.wikipedia.org/wiki/Breadth-first_search) version of the graph traversal language [Gremlin](http://gremlin.tinkerpop.com) operates on a [vertex-centric](http://en.wikipedia.org/wiki/Adjacency_list) property graph data structure. It can be extended with new operations written using [MapReduce](http://hadoop.apache.org/mapreduce/) and [Blueprints](http://blueprints.tinkerpop.com).

== Features

* Support for various graph-based data sources/sinks
  * [JanusGraph](http://janusgraph.org) distributed graph database
    * [Apache Cassandra](http://cassandra.apache.org/)
    * [Apache HBase](http://hbase.apache.org/)
  * [GraphSON](https://github.com/tinkerpop/blueprints/wiki/GraphSON-Reader-and-Writer-Library) text format stored in HDFS
  *  EdgeList multi-relational text format stored in HDFS
    * [RDF](http://www.w3.org/RDF/) text formats stored in HDFS
  * Hadoop binary [sequence files](http://wiki.apache.org/hadoop/SequenceFile) stored in HDFS
  * User defined import/export scripts
* Native integration with the [TinkerPop](http://www.tinkerpop.com) graph stack:
  * [Gremlin](http://gremlin.tinkerpop.com) graph query language
  * [Blueprints](http://blueprints.tinkerpop.com) standard graph API
