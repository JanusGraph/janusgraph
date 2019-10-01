# Removal

!!! warning
    Index removal is a manual process comprised of multiple steps. These
    steps must be carefully followed in the right order to avoid index
    inconsistencies.

## Overview

Index removal is a two-stage process. In the first stage, one JanusGraph
signals to all others via the storage backend that the index is slated
for deletion. This changes the index’s state to `DISABLED`. At that
point, JanusGraph stops using the index to answer queries and stops
incrementally updating the index. Index-related data in the storage
backend remains present but ignored.

The second stage depends on whether the index is mixed or composite. A
composite index can be deleted via JanusGraph. As with reindexing,
removal can be done through either MapReduce or JanusGraphManagement.
However, a mixed index must be manually dropped in the index backend;
JanusGraph does not provide an automated mechanism to delete an index
from its index backend.

Index removal deletes everything associated with the index except its
schema definition and its `DISABLED` state. This schema stub for the
index remains even after deletion, though its storage footprint is
negligible and fixed.

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
-   JanusGraphManagement

Index removal on MapReduce supports large, horizontally-distributed
databases. Index removal on JanusGraphManagement spawns a single-machine
OLAP job. This is intended for convenience and speed on those databases
small enough to be handled by one machine.

Index removal requires:

-   The index name (a string — the user provides this to JanusGraph when
    building a new index)
-   The index type (a string — the name of the edge label or property
    key on which the vertex-centric index is built). This applies only
    to vertex-centric indexes - leave blank for global graph indexes.

As noted in the overview, a mixed index must be manually dropped from
the indexing backend. Neither the MapReduce framework nor the
JanusGraphManagement framework will delete a mixed backend from the
indexing backend.

## Executing an Index Removal Job on MapReduce

As with reindexing, the recommended way to generate and run an index
removal job on MapReduce is through the `MapReduceIndexManagement`
class. Here is a rough outline of the steps to run an index removal job
using this class:

- Open a `JanusGraph` instance
- If the index has not yet been disabled, disable it through `JanusGraphManagement`
- Pass the graph instance into `MapReduceIndexManagement`'s constructor
- Call `updateIndex(<index>, SchemaAction.REMOVE_INDEX)`

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
nameIndex = m.getGraphIndex('name')
m.updateIndex(nameIndex, SchemaAction.DISABLE_INDEX).get()
m.commit()
graph.tx().commit()

// Block until the SchemaStatus transitions from INSTALLED to REGISTERED
ManagementSystem.awaitGraphIndexStatus(graph, 'name').status(SchemaStatus.DISABLED).call()

// Delete the index using MapReduceIndexJobs
m = graph.openManagement()
mr = new MapReduceIndexManagement(graph)
future = mr.updateIndex(m.getGraphIndex('name'), SchemaAction.REMOVE_INDEX)
m.commit()
graph.tx().commit()
future.get()

// Index still shows up in management interface as DISABLED -- this is normal
m = graph.openManagement()
idx = m.getGraphIndex('name')
idx.getIndexStatus(m.getPropertyKey('name'))
m.rollback()

// JanusGraph should issue a warning about this query requiring a full scan
g.V().has('name', 'jupiter')
```

## Executing an Index Removal job on JanusGraphManagement

To run an index removal job on JanusGraphManagement, invoke
`JanusGraphManagement.updateIndex` with the `SchemaAction.REMOVE_INDEX`
argument. For example:
```groovy
m = graph.openManagement()
i = m.getGraphIndex('indexName')
m.updateIndex(i, SchemaAction.REMOVE_INDEX).get()
m.commit()
```

### Example for JanusGraphManagement

The following loads some indexed sample data into a BerkeleyDB-backed
JanusGraph database, then disables and removes the index through
JanusGraphManagement:
```groovy
import org.janusgraph.graphdb.database.management.ManagementSystem

// Load the "Graph of the Gods" sample data
graph = JanusGraphFactory.open('conf/janusgraph-cql-es.properties')
g = graph.traversal()
GraphOfTheGodsFactory.load(graph)

g.V().has('name', 'jupiter')

// Disable the "name" composite index
m = graph.openManagement()
nameIndex = m.getGraphIndex('name')
m.updateIndex(nameIndex, SchemaAction.DISABLE_INDEX).get()
m.commit()
graph.tx().commit()

// Block until the SchemaStatus transitions from INSTALLED to REGISTERED
ManagementSystem.awaitGraphIndexStatus(graph, 'name').status(SchemaStatus.DISABLED).call()

// Delete the index using JanusGraphManagement
m = graph.openManagement()
nameIndex = m.getGraphIndex('name')
future = m.updateIndex(nameIndex, SchemaAction.REMOVE_INDEX)
m.commit()
graph.tx().commit()

future.get()

m = graph.openManagement()
nameIndex = m.getGraphIndex('name')

g.V().has('name', 'jupiter')
```