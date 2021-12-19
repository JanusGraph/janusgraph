# Management System

JanusGraph Management System provides methods to define, update, and inspect the schema of
a JanusGraph graph, and more. Checkout the
JanusGraph Management [API documentation](https://javadoc.io/doc/org.janusgraph/janusgraph-core/{{ latest_version }}/org/janusgraph/core/schema/JanusGraphManagement.html)
for all core APIs available.

JanusGraph Management System behaves like a transaction in that it opens a transactional scope. As such, it needs to
be closed via its commit or rollback methods, unless otherwise specified.
```groovy
mgmt = graph.openManagement()
// do something
mgmt.commit()
```

!!! note
    We **strongly** encourage all users of JanusGraph not to use JanusGraph’s APIs outside of the management system,
    and always use standard Gremlin query language for any queries.

## Schema Management

Management System allows you to view, update, and create vertex labels, edge labels, and property keys.
See [schema management](../schema/index.md) for details.

## Index Management

Management System allows you to manage both vertex-centric indexes and graph indexes. See
[index management](../schema/index-management/index-performance.md) for details.

## Consistency Management

Management System allows you to set the consistency level of individual schema elements. See
[Eventual Consistency](../advanced-topics/eventual-consistency.md) for details.

## Ghost Vertex Removal

Management System allows you to purge [ghost vertices](../advanced-topics/eventual-consistency.md#ghost-vertices)
in the graph. It uses a local thread pool to initiate multiple threads to scan your entire graph, detecting and
purging ghost vertices as well as their incident edges, leveraging
[GhostVertexRemover](https://javadoc.io/doc/org.janusgraph/janusgraph-core/{{ latest_version }}/org/janusgraph/graphdb/olap/job/GhostVertexRemover.html)
By default, the concurrency level is the number of available
processors on your machine. You can also configure the number of threads as shown in the example below. If your graph
is huge, you could consider running GhostVertexRemover on a MapReduce cluster.
```groovy
mgmt = graph.openManagement()
// by default, concurrency level = the number of available processors
mgmt.removeGhostVertices().get()
// alternatively, you could also configure the concurrency
mgmt.removeGhostVertices(4).get()
// it is not necessary to commit here, since GhostVertexRemover commits
// periodically and automatically, but it is a good habit to do so
// calling rollback() won't really rollback the ghost vertex removal process
mgmt.commit()
```
