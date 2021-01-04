# Reindexing

[Graph Index](./index-performance.md#graph-index) and 
[Vertex-centric Indexes](./index-performance.md#vertex-centric-indexes) 
describe how to build
graph-global and vertex-centric indexes to improve query performance.
These indexes are immediately available if the indexed keys or labels
have been newly defined in the same management transaction. In this
case, there is no need to reindex the graph and this section can be
skipped. If the indexed keys and labels already existed prior to index
construction it is necessary to reindex the entire graph in order to
ensure that the index contains previously added elements. This section
describes the reindexing process.

!!! warning
    Reindexing is a manual process comprised of multiple steps. These
    steps must be carefully followed in the right order to avoid index
    inconsistencies.

## Overview

JanusGraph can begin writing incremental index updates right after an
index is defined. However, before the index is complete and usable,
JanusGraph must also take a one-time read pass over all existing graph
elements associated with the newly indexed schema type(s). Once this
reindexing job has completed, the index is fully populated and ready to
be used. The index must then be enabled to be used during query
processing.

## Prior to Reindex

The starting point of the reindexing process is the construction of an
index. Refer to [Indexing for Better Performance](./index-performance.md) for a complete discussion of global
graph and vertex-centric indexes. Note, that a global graph index is
uniquely identified by its name. A vertex-centric index is uniquely
identified by the combination of its name and the edge label or property
key on which the index is defined - the name of the latter is referred
to as the **index type** in this section and only applies to
vertex-centric indexes.

After building a new index against existing schema elements it is
recommended to wait a few minutes for the index to be announced to the
cluster. Note the index name (and the index type in case of a
vertex-centric index) since this information is needed when reindexing.

## Preparing to Reindex

There is a choice between two execution frameworks for reindex jobs:

-   MapReduce
-   JanusGraphManagement

Reindex on MapReduce supports large, horizontally-distributed databases.
Reindex on JanusGraphManagement spawns a single-machine OLAP job. This
is intended for convenience and speed on those databases small enough to
be handled by one machine.

Reindexing requires:

-   The index name (a string — the user provides this to JanusGraph when
    building a new index)
-   The index type (a string — the name of the edge label or property
    key on which the vertex-centric index is built). This applies only
    to vertex-centric indexes - leave blank for global graph indexes.

## Executing a Reindex Job on MapReduce

The recommended way to generate and run a reindex job on MapReduce is
through the `MapReduceIndexManagement` class. Here is a rough outline of
the steps to run a reindex job using this class:

- Open a `JanusGraph` instance
- Pass the graph instance into `MapReduceIndexManagement`'s constructor
- Call `updateIndex(<index>, SchemaAction.REINDEX)` on the `MapReduceIndexManagement` instance
- If the index has not yet been enabled, enable it through `JanusGraphManagement`

This class implements an `updateIndex` method that supports only the
`REINDEX` and `REMOVE_INDEX` actions for its `SchemaAction` parameter.
The class starts a Hadoop MapReduce job using the Hadoop configuration
and jars on the classpath. Both Hadoop 1 and 2 are supported. This class
gets metadata about the index and storage backend (e.g. the Cassandra
partitioner) from the `JanusGraph` instance given to its constructor.
```groovy
graph = JanusGraphFactory.open(...)
mgmt = graph.openManagement()
mr = new MapReduceIndexManagement(graph)
mr.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("battled"), "battlesByTime"), SchemaAction.REINDEX).get()
mgmt.commit()
```

### Reindex Example on MapReduce

The following Gremlin snippet outlines all steps of the MapReduce
reindex process in one self-contained example using minimal dummy data
against the Cassandra storage backend.
```groovy
// Open a graph
graph = JanusGraphFactory.open("conf/janusgraph-cql-es.properties")
g = graph.traversal()

// Define a property
mgmt = graph.openManagement()
desc = mgmt.makePropertyKey("desc").dataType(String.class).make()
mgmt.commit()

// Insert some data
graph.addVertex("desc", "foo bar")
graph.addVertex("desc", "foo baz")
graph.tx().commit()

// Run a query -- note the planner warning recommending the use of an index
g.V().has("desc", containsText("baz"))

// Create an index
mgmt = graph.openManagement()

desc = mgmt.getPropertyKey("desc")
mixedIndex = mgmt.buildIndex("mixedExample", Vertex.class).addKey(desc).buildMixedIndex("search")
mgmt.commit()

// Rollback or commit transactions on the graph which predate the index definition
graph.tx().rollback()

// Block until the SchemaStatus transitions from INSTALLED to REGISTERED
report = ManagementSystem.awaitGraphIndexStatus(graph, "mixedExample").call()

// Run a JanusGraph-Hadoop job to reindex
mgmt = graph.openManagement()
mr = new MapReduceIndexManagement(graph)
mr.updateIndex(mgmt.getGraphIndex("mixedExample"), SchemaAction.REINDEX).get()

// Enable the index
mgmt = graph.openManagement()
mgmt.updateIndex(mgmt.getGraphIndex("mixedExample"), SchemaAction.ENABLE_INDEX).get()
mgmt.commit()

// Block until the SchemaStatus is ENABLED
mgmt = graph.openManagement()
report = ManagementSystem.awaitGraphIndexStatus(graph, "mixedExample").status(SchemaStatus.ENABLED).call()
mgmt.rollback()

// Run a query -- JanusGraph will use the new index, no planner warning
g.V().has("desc", containsText("baz"))

// Concerned that JanusGraph could have read cache in that last query, instead of relying on the index?
// Start a new instance to rule out cache hits.  Now we're definitely using the index.
graph.close()
graph = JanusGraphFactory.open("conf/janusgraph-cql-es.properties")
g.V().has("desc", containsText("baz"))
```

## Executing a Reindex job on JanusGraphManagement

To run a reindex job on JanusGraphManagement, invoke
`JanusGraphManagement.updateIndex` with the `SchemaAction.REINDEX`
argument. For example:
```groovy
m = graph.openManagement()
i = m.getGraphIndex('indexName')
m.updateIndex(i, SchemaAction.REINDEX).get()
m.commit()
```

### Example for JanusGraphManagement

The following loads some sample data into a BerkeleyDB-backed JanusGraph
database, defines an index after the fact, reindexes using
JanusGraphManagement, and finally enables and uses the index:
```groovy
import org.janusgraph.graphdb.database.management.ManagementSystem

// Load some data from a file without any predefined schema
graph = JanusGraphFactory.open('conf/janusgraph-berkeleyje.properties')
g = graph.traversal()
m = graph.openManagement()
m.makePropertyKey('name').dataType(String.class).cardinality(Cardinality.LIST).make()
m.makePropertyKey('lang').dataType(String.class).cardinality(Cardinality.LIST).make()
m.makePropertyKey('age').dataType(Integer.class).cardinality(Cardinality.LIST).make()
m.commit()
graph.io(IoCore.gryo()).readGraph('data/tinkerpop-modern.gio')
graph.tx().commit()

// Run a query -- note the planner warning recommending the use of an index
g.V().has('name', 'lop')
graph.tx().rollback()

// Create an index
m = graph.openManagement()
m.buildIndex('names', Vertex.class).addKey(m.getPropertyKey('name')).buildCompositeIndex()
m.commit()
graph.tx().commit()

// Block until the SchemaStatus transitions from INSTALLED to REGISTERED
ManagementSystem.awaitGraphIndexStatus(graph, 'names').status(SchemaStatus.REGISTERED).call()

// Reindex using JanusGraphManagement
m = graph.openManagement()
i = m.getGraphIndex('names')
m.updateIndex(i, SchemaAction.REINDEX)
m.commit()

// Enable the index
ManagementSystem.awaitGraphIndexStatus(graph, 'names').status(SchemaStatus.ENABLED).call()

// Run a query -- JanusGraph will use the new index, no planner warning
g.V().has('name', 'lop')
graph.tx().rollback()

// Concerned that JanusGraph could have read cache in that last query, instead of relying on the index?
// Start a new instance to rule out cache hits.  Now we're definitely using the index.
graph.close()
graph = JanusGraphFactory.open("conf/janusgraph-berkeleyje.properties")
g = graph.traversal()
g.V().has('name', 'lop')
```

## Common problems

### IllegalArgumentException when starting job

When a reindexing job is started shortly after a the index has been
built, the job might fail with an exception like one of the following:

    The index mixedExample is in an invalid state and cannot be indexed.
    The following index keys have invalid status: desc has status INSTALLED
    (status must be one of [REGISTERED, ENABLED])

    The index mixedExample is in an invalid state and cannot be indexed.
    The index has status INSTALLED, but one of [REGISTERED, ENABLED] is required

When an index is built, its existence is broadcast to all other
JanusGraph instances in the cluster. Those must acknowledge the
existence of the index before the reindexing process can be started. The
acknowledgments can take a while to come in depending on the size of the
cluster and the connection speed. Hence, one should wait a few minutes
after building the index and before starting the reindex process.

Note, that the acknowledgment might fail due to JanusGraph instance
failure. In other words, the cluster might wait indefinitely on the
acknowledgment of a failed instance. In this case, the user must
manually remove the failed instance from the cluster registry as
described in [Failure & Recovery](../../operations/recovery.md). After the cluster state has been
restored, the acknowledgment process must be reinitiated by manually
registering the index again in the management system.

```groovy
mgmt = graph.openManagement()
rindex = mgmt.getRelationIndex(mgmt.getRelationType("battled"),"battlesByTime")
mgmt.updateIndex(rindex, SchemaAction.REGISTER_INDEX).get()
gindex = mgmt.getGraphIndex("byName")
mgmt.updateIndex(gindex, SchemaAction.REGISTER_INDEX).get()
mgmt.commit()
```

After waiting a few minutes for the acknowledgment to arrive the reindex
job should start successfully.

### Could not find index

This exception in the reindexing job indicates that an index with the
given name does not exist or that the name has not been specified
correctly. When reindexing a global graph index, only the name of the
index as defined when building the index should be specified. When
reindexing a global graph index, the name of the index must be given in
addition to the name of the edge label or property key on which the
vertex-centric index is defined.
