Common Questions
================

Accidental type creation
------------------------

By default, JanusGraph will automatically create property keys and edge
labels when a new type is encountered. It is strongly encouraged that
users explicitly schemata as documented in [Schema and Data Modeling](schema.md) before loading any data and disable automatic type
creation by setting the option `schema.default = none`.

Automatic type creation can cause problems in multi-threaded or highly
concurrent environments. Since JanusGraph needs to ensure that types are
unique, multiple attempts at creating the same type will lead to locking
or other exceptions. It is generally recommended to create all needed
types up front or in one batch when new property keys and edge labels
are needed.

Custom Class Datatype
---------------------

JanusGraph supports arbitrary objects as attribute values on properties.
To use a custom class as data type in JanusGraph, either register a
custom serializer or ensure that the class has a no-argument constructor
and implements the `equals` method because JanusGraph will verify that
it can successfully de-/serialize objects of that class. Please see
[Datatype and Attribute Serializer Configuration](../advanced-topics/serializer.md) for more information.

Transactional Scope for Edges
-----------------------------

Edges should not be accessed outside the scope in which they were
originally created or retrieved.

Locking Exceptions
------------------

When defining unique types with [locking enabled](../advanced-topics/eventual-consistency.md)
(i.e. requesting that JanusGraph ensures uniqueness) it is likely to
encounter locking exceptions of the type `PermanentLockingException`
under concurrent modifications to the graph.

Such exceptions are to be expected, since JanusGraph cannot know how to
recover from a transactional state where an earlier read value has been
modified by another transaction since this may invalidate the state of
the transaction. In most cases it is sufficient to simply re-run the
transaction. If locking exceptions are very frequent, try to analyze and
remove the source of congestion.

Ghost Vertices
--------------

When the same vertex is concurrently removed in one transaction and
modified in another, both transactions will successfully commit on
eventually consistent storage backends and the vertex will still exist
with only the modified properties or edges. This is referred to as a
ghost vertex. It is possible to guard against ghost vertices on
eventually consistent backends using key [uniqueness](#index-unique) but
this is prohibitively expensive in most cases. A more scalable approach
is to allow ghost vertices temporarily and clearing them out in regular
time intervals.

Another option is to detect them at read-time using the option
`checkInternalVertexExistence()` documented in [Transaction Configuration](#tx-config).

Debug-level Logging Slows Execution
-----------------------------------

When the log level is set to `DEBUG` JanusGraph produces **a lot** of
logging output which is useful to understand how particular queries get
compiled, optimized, and executed. However, the output is so large that
it will impact the query performance noticeably. Hence, use `INFO`
severity or higher for production systems or benchmarking.

JanusGraph OutOfMemoryException or excessive Garbage Collection
---------------------------------------------------------------

If you experience memory issues or excessive garbage collection while
running JanusGraph it is likely that the caches are configured
incorrectly. If the caches are too large, the heap may fill up with
cache entries. Try reducing the size of the transaction level cache
before tuning the database level cache, in particular if you have many
concurrent transactions. See [JanusGraph Cache](cache.md) for more
information.

JAMM Warning Messages
---------------------

When launching JanusGraph with embedded Cassandra, the following
warnings may be displayed:

`958 [MutationStage:25] WARN  org.apache.cassandra.db.Memtable  - MemoryMeter uninitialized (jamm not specified as java agent); assuming liveRatio of 10.0.  Usually this means cassandra-env.sh disabled jamm because you are using a buggy JRE; upgrade to the Sun JRE instead`

Cassandra uses a Java agent called `MemoryMeter` which allows it to
measure the actual memory use of an object, including JVM overhead. To
use [JAMM](https://github.com/jbellis/jamm) (Java Agent for Memory
Measurements), the path to the JAMM jar must be specific in the Java
javaagent parameter when launching the JVM (e.g.
`-javaagent:path/to/jamm.jar`) through either `janusgraph.sh`,
`gremlin.sh`, or Gremlin Server:

    export JANUSGRAPH_JAVA_OPTS=-javaagent:$JANUSGRAPH_HOME/lib/jamm-{{ jamm_version }}.jar

Cassandra Connection Problem
----------------------------

By default, JanusGraph uses the Astyanax library to connect to Cassandra
clusters. On EC2 and Rackspace, it has been reported that Astyanax was
unable to establish a connection to the cluster. In those cases,
changing the backend to `storage.backend=cassandrathrift` solved the
problem.

Elasticsearch OutOfMemoryException
----------------------------------

When numerous clients are connecting to Elasticsearch, it is likely that
an `OutOfMemoryException` occurs. This is not due to a memory issue, but
to the OS not allowing more threads to be spawned by the user (the user
running Elasticsearch). To circumvent this issue, increase the number of
allowed processes to the user running Elasticsearch. For example,
increase the `ulimit -u` from the default 1024 to 10024.

Dropping a Database
-------------------

To drop a database using the Gremlin Console you can call
`JanusGraphFactory.drop(graph)`. The graph you want to drop needs to be
defined prior to running the drop method.

With ConfiguredGraphFactory
```groovy
graph = ConfiguredGraphFactory.open('example')
ConfiguredGraphFactory.drop('example');
```

With JanusGraphFactory
```groovy
graph = JanusGraphFactory.open('path/to/configuration.properties')
JanusGraphFactory.drop(graph);
```

Note that on JanusGraph versions prior to 0.3.0 if multiple Gremlin
Server instances are connecting to the graph that has been dropped it is
reccomended to close the graph on all active nodes by running either
`JanusGraphFactory.close(graph)` or
`ConfiguredGraphFactory.close("example")` depending on which graph
manager is in use. Closing and reopening the graph on all active nodes
will prevent cached(stale) references to the graph that has been
dropped. ConfiguredGraphFactory graphs that are dropped may need to have
their configurations recreated using the [graph configuration singleton](configured-graph-factory.md#graph-configurations) or
[template configuration](configured-graph-factory.md#template-configuration).

