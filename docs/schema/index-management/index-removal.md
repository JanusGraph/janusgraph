# Removal

!!! warning
    Index removal is a manual process comprised of multiple steps. These
    steps must be carefully followed in the right order to avoid index
    inconsistencies.

## Overview

Index removal is a multi-stage process.
In the first stage, one JanusGraph instance deactivates the index by setting its state to `DISABLED`.
At that point, JanusGraph stops using the index to answer queries and stops incrementally updating the index.
Index-related data in the storage backend remains present but ignored.

The second stage depends on whether the index is mixed or composite.
All composite indices can be deleted via JanusGraph.
As with reindexing, removal can be done through either MapReduce or JanusGraphManagement.
However, not all mixed index backends allow automatic removal.
Index backends for which JanusGraph does not provide an automated deletion mechanism — currently Lucene and
Solr — must be manually dropped in the index backend.
For supported mixed index backends, the removal process is analogue to the composite index removal,
with the exception that neither MapReduce nor multi threading in JanusGraphManagement is necessary.

Discarding an index deletes everything associated with the index except its schema definition and its `DISCARDED` state.
Removing this schema stub for the index is the third step.

## Preparing for Index Removal

If the index is currently enabled, it should first be disabled. This is
done through the `ManagementSystem`.
```groovy
mgmt = graph.openManagement()
rindex = mgmt.getRelationIndex(mgmt.getRelationType("battled"), "battlesByTime")
mgmt.updateIndex(rindex, SchemaAction.DISABLE_INDEX).get()
gindex = mgmt.getGraphIndex("byName")
mgmt.updateIndex(gindex, SchemaAction.DISABLE_INDEX).get()
mgmt.commit()
```

Once the status of all keys on the index changes to `DISABLED`, the
index is ready to be removed. A utility in ManagementSystem can automate
the wait-for-`DISABLED` step:
```groovy
ManagementSystem.awaitGraphIndexStatus(graph, 'byName').status(SchemaStatus.DISABLED).call()
```

After a composite index is `DISABLED`, there is a choice between two
execution frameworks for its removal:

-   MapReduce
-   ManagementSystem

Index removal on MapReduce supports large, horizontally-distributed
databases. Index removal on ManagementSystem spawns a single-machine
OLAP job. This is intended for convenience and speed on those databases
small enough to be handled by one machine.

Index removal requires:

-   The index name (a string — the user provides this to JanusGraph when
    building a new index)
-   The index type (a string — the name of the edge label or property
    key on which the vertex-centric index is built). This applies only
    to vertex-centric indices - leave blank for global graph indices.
 
## Executing an index removal job on MapReduce

!!! info
    This method applies to the following types of indices: 

    - All composite indices

As with reindexing, the recommended way to generate and run an index
removal job on MapReduce is through the `MapReduceIndexManagement`
class. Here is a rough outline of the steps to run an index removal job
using this class:

- Open a `JanusGraph` instance
- If the index has not yet been disabled, disable it through `JanusGraphManagement`
- Pass the graph instance into `MapReduceIndexManagement`'s constructor
- Call `updateIndex(<index>, SchemaAction.DISCARD_INDEX)`

A commented code example follows in the next subsection.

### Example for MapReduce

```groovy
import org.janusgraph.graphdb.database.management.ManagementSystem

// Load the "Graph of the Gods" sample data
graph = JanusGraphFactory.open('conf/janusgraph-cql-es.properties')
g = graph.traversal()
GraphOfTheGodsFactory.load(graph)

g.V().has('name', 'jupiter')

// Disable the "name" composite index
m = graph.openManagement()
m.updateIndex(m.getGraphIndex('name'), SchemaAction.DISABLE_INDEX).get()
m.commit()
graph.tx().commit()

// Block until the SchemaStatus transitions from ENABLED to DISABLED
ManagementSystem.awaitGraphIndexStatus(graph, 'name').status(SchemaStatus.DISABLED).call()

// Delete the indexed data using MapReduceIndexJobs
m = graph.openManagement()
mr = new MapReduceIndexManagement(graph)
future = mr.updateIndex(m.getGraphIndex('name'), SchemaAction.DISCARD_INDEX)
m.commit()
graph.tx().commit()
future.get()

// Block until the SchemaStatus transitions from DISABLED to DISCARDED
ManagementSystem.awaitGraphIndexStatus(graph, 'name').status(SchemaStatus.DISCARDED).call()

// Index still shows up in management interface as DISCARDED -- it can now be dropped entirely
m = graph.openManagement()
m.updateIndex(m.getGraphIndex('name'), SchemaAction.DROP_INDEX).get()
m.commit()

// JanusGraph should issue a warning about this query requiring a full scan
g.V().has('name', 'jupiter')
```

## Executing an index removal job on ManagementSystem

!!! info
    This method applies to the following types of indices:

    - All composite indices
    - Mixed indices (Elasticsearch only)

To run an index removal job on ManagementSystem, invoke
`ManagementSystem.updateIndex` with the `SchemaAction.DISCARD_INDEX`
argument. For example:
```groovy
m = graph.openManagement()
m.updateIndex(m.getGraphIndex('indexName'), SchemaAction.DISCARD_INDEX).get()
m.commit()
```

Similar to reindex, ManagementSystem uses a local thread pool to
execute index removal job concurrently. The concurrency level is
equal to the number of available processors. If you want to change the
default concurrency level, you can add a parameter as follows:
```groovy
// Use only one thread to execute index removal job
m.updateIndex(m.getGraphIndex('indexName'), SchemaAction.DISCARD_INDEX, 1).get()
```

### Example for ManagementSystem

The following loads some indexed sample data into a BerkeleyDB-backed
JanusGraph database, then disables and removes the index through
ManagementSystem:
```groovy
import org.janusgraph.graphdb.database.management.ManagementSystem

// Load the "Graph of the Gods" sample data
graph = JanusGraphFactory.open('conf/janusgraph-cql-es.properties')
g = graph.traversal()
GraphOfTheGodsFactory.load(graph)

g.V().has('name', 'jupiter')

// Disable the "name" composite index
m = graph.openManagement()
m.updateIndex(m.getGraphIndex('name'), SchemaAction.DISABLE_INDEX).get()
m.commit()
graph.tx().commit()

// Block until the SchemaStatus transitions from ENABLED to DISABLED
ManagementSystem.awaitGraphIndexStatus(graph, 'name').status(SchemaStatus.DISABLED).call()

// Delete the index using JanusGraphManagement
m = graph.openManagement()
future = m.updateIndex(m.getGraphIndex('name'), SchemaAction.DISCARD_INDEX)
m.commit()
graph.tx().commit()
future.get()

// Block until the SchemaStatus transitions from DISABLED to DISCARDED
ManagementSystem.awaitGraphIndexStatus(graph, 'name').status(SchemaStatus.DISCARDED).call()

// Index still shows up in management interface as DISCARDED -- it can now be dropped entirely
m = graph.openManagement()
m.updateIndex(m.getGraphIndex('name'), SchemaAction.DROP_INDEX).get()
m.commit()

// JanusGraph should issue a warning about this query requiring a full scan
g.V().has('name', 'jupiter')
```

## Executing a manual index removal for mixed indices

!!! info
    This method applies to the following types of indices:

    - Mixed indices which can not be removed by Janusgraph automatically

If an index backend does not support automatic removal, you will receive an exception when trying to execute
`DISCARD_INDEX`.
This currently applies to all mixed indices except for Elasticsearch.
For those index backends, indices have to be removed by hand in the index backend without the help of JanusGraph.
In order to inform JanusGraph that an index has been removed manually, the transition to the state `DISCARDED` can be
manually triggered by executing `MARK_DISCARDED`.

```groovy
m = graph.openManagement()
m.updateIndex(m.getGraphIndex('indexName'), SchemaAction.MARK_DISCARDED).get()
m.commit()
```

!!! warning
    This action does not execute any operation except overwriting the state of the index.
    Any data stored in the index backend will still be present.

It is recommended to execute this step before actually touching the backend and deleting the data.
This way, it is guaranteed that no instance concurrently re-enables the index.
Once the index has entered the state `DISCARDED`, all indexed data can be safely deleted.

### Example for manual mixed index removal
```groovy
import org.janusgraph.graphdb.database.management.ManagementSystem

// Load the "Graph of the Gods" sample data
graph = JanusGraphFactory.open('conf/janusgraph-cql-es.properties')
g = graph.traversal()
GraphOfTheGodsFactory.load(graph)

g.V().has("name", "jupiter")

// Disable the "name" composite index
m = graph.openManagement()
m.updateIndex(m.getGraphIndex("name"), SchemaAction.DISABLE_INDEX).get()
m.commit()
graph.tx().commit()

// Block until the SchemaStatus transitions from ENABLED to DISABLED
ManagementSystem.awaitGraphIndexStatus(graph, "name").status(SchemaStatus.DISABLED).call()

// Since JanusGraph is not capable of removing the index, we have to set the state manually.
m = graph.openManagement()
future = m.updateIndex(m.getGraphIndex("name"), SchemaAction.MARK_DISCARDED)
m.commit()
graph.tx().commit()
future.get()

// Block until the SchemaStatus transitions from DISABLED to DISCARDED
ManagementSystem.awaitGraphIndexStatus(graph, "name").status(SchemaStatus.DISCARDED).call()

/*
 * At this point, the index is marked as DISCARDED,
 * so it is ensured that no instance will ever be
 * able to use it again. You are now safe to manually
 * perform the necessary operations in your index
 * backend to remove the indexed data by hand.
 */

// Index still shows up in management interface as DISCARDED -- it can now be dropped entirely
m = graph.openManagement()
m.updateIndex(m.getGraphIndex("name"), SchemaAction.DROP_INDEX).get()
m.commit()

// JanusGraph should issue a warning about this query requiring a full scan
g.V().has("name", "jupiter")
```
