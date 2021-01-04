# Advanced Schema

This page describes some of the advanced schema definition options that
JanusGraph provides. For general information on JanusGraph’s schema and
how to define it, refer to [Schema and Data Modeling](../schema/index.md).

## Static Vertices

Vertex labels can be defined as **static** which means that vertices
with that label cannot be modified outside the transaction in which they
were created.
```groovy
mgmt = graph.openManagement()
tweet = mgmt.makeVertexLabel('tweet').setStatic().make()
mgmt.commit()
```

Static vertex labels are a method of controlling the data lifecycle and
useful when loading data into the graph that should not be modified
after its creation.

## Edge and Vertex TTL

Edge and vertex labels can be configured with a **time-to-live (TTL)**.
Edges and vertices with such labels will automatically be removed from
the graph when the configured TTL has passed after their initial
creation. TTL configuration is useful when loading a large amount of
data into the graph that is only of temporary use. Defining a TTL
removes the need for manual clean up and handles the removal very
efficiently. For example, it would make sense to TTL event edges such as
user-page visits when those are summarized after a certain period of
time or simply no longer needed for analytics or operational query
processing.

The following storage backends support edge and vertex TTL.

-   CQL compatible storage backends
-   HBase
-   BerkeleyDB - supports only hour-discrete TTL, thus the minimal TTL is one hour.

### Edge TTL

Edge TTL is defined on a per-edge label basis, meaning that all edges of
that label have the same time-to-live. Note that the backend must
support cell level TTL. Currently only CQL, HBase and BerkeleyDB support this.
```groovy
mgmt = graph.openManagement()
visits = mgmt.makeEdgeLabel('visits').make()
mgmt.setTTL(visits, Duration.ofDays(7))
mgmt.commit()
```

Note, that modifying an edge resets the TTL for that edge. Also note,
that the TTL of an edge label can be modified but it might take some
time for this change to propagate to all running JanusGraph instances
which means that two different TTLs can be temporarily in use for the
same label.

### Property TTL

Property TTL is very similar to edge TTL and defined on a per-property
key basis, meaning that all properties of that key have the same
time-to-live. Note that the backend must support cell level TTL.
Currently only CQL, HBase and BerkeleyDB support this.
```groovy
mgmt = graph.openManagement()
sensor = mgmt.makePropertyKey('sensor').cardinality(Cardinality.LIST).dataType(Double.class).make()
mgmt.setTTL(sensor, Duration.ofDays(21))
mgmt.commit()
```

As with edge TTL, modifying an existing property resets the TTL for that
property and modifying the TTL for a property key might not immediately
take effect.

### Vertex TTL

Vertex TTL is defined on a per-vertex label basis, meaning that all
vertices of that label have the same time-to-live. The configured TTL
applies to the vertex, its properties, and all incident edges to ensure
that the entire vertex is removed from the graph. For this reason, a
vertex label must be defined as *static* before a TTL can be set to rule
out any modifications that would invalidate the vertex TTL. Vertex TTL
only applies to static vertex labels. Note that the backend must support
store level TTL. Currently only CQL, HBase and BerkeleyDB support this.
```groovy
mgmt = graph.openManagement()
tweet = mgmt.makeVertexLabel('tweet').setStatic().make()
mgmt.setTTL(tweet, Duration.ofHours(36))
mgmt.commit()
```

Note, that the TTL of a vertex label can be modified but it might take
some time for this change to propagate to all running JanusGraph
instances which means that two different TTLs can be temporarily in use
for the same label.

## Multi-Properties

As discussed in [Schema and Data Modeling](../schema/index.md), JanusGraph supports property keys with
SET and LIST cardinality. Hence, JanusGraph supports multiple properties
with the same key on a single vertex. Furthermore, JanusGraph treats
properties similarly to edges in that single-valued property annotations
are allowed on properties as shown in the following example.
```groovy
mgmt = graph.openManagement()
mgmt.makePropertyKey('name').dataType(String.class).cardinality(Cardinality.LIST).make()
mgmt.commit()
v = graph.addVertex()
p1 = v.property('name', 'Dan LaRocque')
p1.property('source', 'web')
p2 = v.property('name', 'dalaro')
p2.property('source', 'github')
graph.tx().commit()
v.properties('name')
==> Iterable over all name properties
```
These features are useful in a number of applications such as those
where attaching provenance information (e.g. who added a property, when
and from where?) to properties is necessary. Support for higher
cardinality properties and property annotations on properties is also
useful in high-concurrency, scale-out design patterns as described in
[Eventually-Consistent Storage Backends](../advanced-topics/eventual-consistency.md).

Vertex-centric indexes and global graph indexes are supported for
properties in the same manner as they are supported for edges. Refer to
[Indexing for Better Performance](index-management/index-performance.md) for information on defining these indexes for edges and
use the corresponding API methods to define the same indexes for
properties.

## Unidirected Edges

Unidirected edges are edges that can only be traversed in the out-going
direction. Unidirected edges have a lower storage footprint but are
limited in the types of traversals they support. Unidirected edges are
conceptually similar to hyperlinks in the world-wide-web in the sense
that the out-vertex can traverse through the edge, but the in-vertex is
unaware of its existence.
```groovy
mgmt = graph.openManagement()
mgmt.makeEdgeLabel('author').unidirected().make()
mgmt.commit()
```

Note, that unidirected edges do not get automatically deleted when their
in-vertices are deleted. The user must ensure that such inconsistencies
do not arise or resolve them at query time by explicitly checking vertex
existence in a transaction. See the discussion in [Ghost Vertices](../common-questions.md#ghost-vertices)
for more information.
