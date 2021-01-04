# Technical Limitations

There are various limitations and "gotchas" that one should be aware of
when using JanusGraph. Some of these limitations are necessary design
choices and others are issues that will be rectified as JanusGraph
development continues. Finally, the last section provides solutions to
common issues.

## Design Limitations

These limitations reflect long-term tradeoffs design tradeoffs which are
either difficult or impractical to change. These limitations are
unlikely to be removed in the near future.

### Size Limitation

JanusGraph can store up to a quintillion edges (2^60) and half as many
vertices. That limitation is imposed by JanusGraph’s id scheme.

### DataType Definitions

When declaring the data type of a property key using `dataType(Class)`
JanusGraph will enforce that all properties for that key have the
declared type, unless that type is `Object.class`. This is an equality
type check, meaning that sub-classes will not be allowed. For instance,
one cannot declare the data type to be `Number.class` and use `Integer`
or `Long`. For efficiency reasons, the type needs to match exactly.
Hence, use `Object.class` as the data type for type flexibility. In all
other cases, declare the actual data type to benefit from increased
performance and type safety.

### Edge Retrievals are O(log(k))

Retrieving an edge by id, e.g `tx.getEdge(edge.getId())`, is not a
constant time operation because it requires an index call on one of its
adjacent vertices. Hence, the cost of retrieving an individual edge by
its id is `O(log(k))` where `k` is the number of incident edges on the
adjacent vertex. JanusGraph will attempt to pick the adjacent vertex
with the smaller degree.

This also applies to index retrievals for edges via a standard or
external index.

### Type Definitions cannot be changed

The definition of an edge label, property key, or vertex label cannot be
changed once it has been committed to the graph. However, a type can be
renamed and new types can be created at runtime to accommodate an
evolving schema.

### Reserved Keywords

There are certain keywords that JanusGraph uses internally for types
that cannot be used otherwise. These types include vertex labels, edge
labels, and property keys. The following are keywords that cannot be
used:

-   vertex

-   element

-   edge

-   property

-   label

-   key

For example, if you attempt to create a vertex with the label of
`property`, you will receive an exception regarding protected system
types.

## Temporary Limitations

These are limitations in JanusGraph’s current implementation. These
limitations could reasonably be removed in upcoming versions of
JanusGraph.

### Limited Mixed Index Support

Mixed indexes only support a subset of the data types that JanusGraph
supports. See [Mixed Index Data Types](../interactions/search-predicates.md#data-type-support) for a current
listing. Also, mixed indexes do not currently support property keys with
SET or LIST cardinality.

### Batch Loading Speed

JanusGraph provides a batch loading mode that can be enabled through the
[graph configuration](../configs/configuration-reference.md). However, this batch mode only
facilitates faster loading into the storage backend, it does not use
storage backend specific batch loading techniques that prepare the data
in memory for disk storage. As such, batch loading in JanusGraph is
currently slower than batch loading modes provided by single machine
databases. [Bulk Loading](../operations/bulk-loading.md) contains information on speeding up
batch loading in JanusGraph.

Another limitation related to batch loading is the failure to load
millions of edges into a single vertex at once or in a short time of
period. Such **supernode loading** can fail for some storage backends.
This limitation also applies to dense index entries.
