# Indexing for Better Performance

JanusGraph supports two different kinds of indexing to speed up query
processing: **graph indexes** and **vertex-centric indexes (a.k.a. relation indexes)**.
Most graph queries start the traversal from a list of vertices or edges that are
identified by their properties. Graph indexes make these global
retrieval operations efficient on large graphs. Vertex-centric indexes
speed up the actual traversal through the graph, in particular when
traversing through vertices with many incident edges.

## Graph Index

Graph indexes are global index structures over the entire graph which
allow efficient retrieval of vertices or edges by their properties for
sufficiently selective conditions. For instance, consider the following
queries
```java
g.V().has('name', 'hercules')
g.E().has('reason', textContains('loves'))
```

The first query asks for all vertices with the name `hercules`. The
second asks for all edges where the property reason contains the word
`loves`. Without a graph index answering those queries would require a
full scan over all vertices or edges in the graph to find those that
match the given condition which is very inefficient and infeasible for
huge graphs.

JanusGraph distinguishes between two types of graph indexes:
**composite** and **mixed** indexes. Composite indexes are very fast and
efficient but limited to equality lookups for a particular,
previously-defined combination of property keys. Mixed indexes can be
used for lookups on any combination of indexed keys and support multiple
condition predicates in addition to equality depending on the backing
index store.

Both types of indexes are created through the JanusGraph management
system and the index builder returned by
`JanusGraphManagement.buildIndex(String, Class)` where the first
argument defines the name of the index and the second argument specifies
the type of element to be indexed (e.g. `Vertex.class`). The name of a
graph index must be unique. Graph indexes built against newly defined
property keys, i.e. property keys that are defined in the same
management transaction as the index, are immediately available. The same
applies to graph indexes that are constrained to a label that is created
in the same management transaction as the index. Graph indexes built
against property keys that are already in use without being constrained
to a newly created label require the execution of a [reindex procedure](./index-reindexing.md) 
to ensure that the index contains all previously
added elements. Until the reindex procedure has completed, the index
will not be available. It is encouraged to define graph indexes in the
same transaction as the initial schema.

!!! note
    In the absence of an index, JanusGraph will default to a full graph
    scan in order to retrieve the desired list of vertices. While this
    produces the correct result set, the graph scan can be very
    inefficient and lead to poor overall system performance in a
    production environment. Enable the `force-index` configuration option
    in production deployments of JanusGraph to prohibit graph scans.

!!! Info
    See [index lifecycle documentation](./index-lifecycle.md) for more information about index states.

### Composite Index

Composite indexes are stored in a separate store called `graphIndex`.
For example, if your storage backend is Cassandra, you will see a table
named `graphIndex` under your namespace.
Composite indexes retrieve vertices or edges by one or a (fixed)
composition of multiple keys. Consider the following composite index
definitions.
```java
graph.tx().rollback() //Never create new indexes while a transaction is active
mgmt = graph.openManagement()
name = mgmt.getPropertyKey('name')
age = mgmt.getPropertyKey('age')
mgmt.buildIndex('byNameComposite', Vertex.class).addKey(name).buildCompositeIndex()
mgmt.buildIndex('byNameAndAgeComposite', Vertex.class).addKey(name).addKey(age).buildCompositeIndex()
mgmt.commit()
//Wait for the index to become available
ManagementSystem.awaitGraphIndexStatus(graph, 'byNameComposite').call()
ManagementSystem.awaitGraphIndexStatus(graph, 'byNameAndAgeComposite').call()
//Reindex the existing data
mgmt = graph.openManagement()
mgmt.updateIndex(mgmt.getGraphIndex("byNameComposite"), SchemaAction.REINDEX).get()
mgmt.updateIndex(mgmt.getGraphIndex("byNameAndAgeComposite"), SchemaAction.REINDEX).get()
mgmt.commit()
```

First, two property keys `name` and `age` are already defined. Next, a
simple composite index on just the name property key is built.
JanusGraph will use this index to answer the following query.
```groovy
g.V().has('name', 'hercules')
```

The second composite graph index includes both keys. JanusGraph will use
this index to answer the following query.
```groovy
g.V().has('age', 30).has('name', 'hercules')
```

Note, that all keys of a composite graph index must be found in the
query’s equality conditions for this index to be used. For example, the
following query cannot be answered with either of the indexes because it
only contains a constraint on `age` but not `name`.
```groovy
g.V().has('age', 30)
```

Also note, that composite graph indexes can only be used for equality
constraints like those in the queries above. The following query would
be answered with just the simple composite index defined on the `name`
key because the age constraint is not an equality constraint.
```java
g.V().has('name', 'hercules').has('age', inside(20, 50))
```

Composite indexes do not require configuration of an external indexing
backend and are supported through the primary storage backend. Hence,
composite index modifications are persisted through the same transaction
as graph modifications which means that those changes are atomic and/or
consistent if the underlying storage backend supports atomicity and/or
consistency.

!!! note
    A composite index may comprise just one or multiple keys. A composite
    index with just one key is sometimes referred to as a key-index.

#### Index Uniqueness

Composite indexes can also be used to enforce property uniqueness in the
graph. If a composite graph index is defined as `unique()` there can be
at most one vertex or edge for any given concatenation of property
values associated with the keys of that index. For instance, to enforce
that names are unique across the entire graph the following composite
graph index would be defined.
```groovy
graph.tx().rollback()  //Never create new indexes while a transaction is active
mgmt = graph.openManagement()
name = mgmt.getPropertyKey('name')
mgmt.buildIndex('byNameUnique', Vertex.class).addKey(name).unique().buildCompositeIndex()
mgmt.commit()
//Wait for the index to become available
ManagementSystem.awaitGraphIndexStatus(graph, 'byNameUnique').call()
//Reindex the existing data
mgmt = graph.openManagement()
mgmt.updateIndex(mgmt.getGraphIndex("byNameUnique"), SchemaAction.REINDEX).get()
mgmt.commit()
```

!!! note
    To enforce uniqueness against an eventually consistent storage
    backend, the [consistency](../../advanced-topics/eventual-consistency.md) of the index must be
    explicitly set to enabling locking.

### Mixed Index

Mixed indexes retrieve vertices or edges by any combination of
previously added property keys. Mixed indexes provide more flexibility
than composite indexes and support additional condition predicates
beyond equality. On the other hand, mixed indexes are slower for most
equality queries than composite indexes.

Unlike composite indexes, mixed indexes require the configuration of an
[indexing backend](../../index-backend/index.md) and use that indexing backend to
execute lookup operations. JanusGraph can support multiple indexing
backends in a single installation. Each indexing backend must be
uniquely identified by name in the JanusGraph configuration which is
called the **indexing backend name**.
```groovy
graph.tx().rollback()  //Never create new indexes while a transaction is active
mgmt = graph.openManagement()
name = mgmt.getPropertyKey('name')
age = mgmt.getPropertyKey('age')
mgmt.buildIndex('nameAndAge', Vertex.class).addKey(name).addKey(age).buildMixedIndex("search")
mgmt.commit()
//Wait for the index to become available
ManagementSystem.awaitGraphIndexStatus(graph, 'nameAndAge').call()
//Reindex the existing data
mgmt = graph.openManagement()
mgmt.updateIndex(mgmt.getGraphIndex("nameAndAge"), SchemaAction.REINDEX).get()
mgmt.commit()
```

The example above defines a mixed index containing the property keys
`name` and `age`. The definition refers to the indexing backend name
`search` so that JanusGraph knows which configured indexing backend it
should use for this particular index. The `search` parameter specified
in the buildMixedIndex call must match the second clause in the
JanusGraph configuration definition like this: index.**search**.backend
If the index was named *solrsearch* then the configuration definition
would appear like this: index.**solrsearch**.backend.

The mgmt.buildIndex example specified above uses text search as its
default behavior. An index statement that explicitly defines the index
as a text index can be written as follows:
```groovy
mgmt.buildIndex('nameAndAge',Vertex.class).addKey(name,Mapping.TEXT.asParameter()).addKey(age,Mapping.TEXT.asParameter()).buildMixedIndex("search")
```

See [Index Parameters and Full-Text Search](../../index-backend/text-search.md) for more information on text and string
search options, and see the documentation section specific to the
indexing backend in use for more details on how each backend handles
text versus string searches.

While the index definition example looks similar to the composite index
above, it provides greater query support and can answer *any* of the
following queries.
```groovy
g.V().has('name', textContains('hercules')).has('age', inside(20, 50))
g.V().has('name', textContains('hercules'))
g.V().has('age', lt(50))
g.V().has('age', outside(20, 50))
g.V().has('age', lt(50).or(gte(60)))
g.V().or(__.has('name', textContains('hercules')), __.has('age', inside(20, 50)))
```

Mixed indexes support full-text search, range search, geo search and
others. Refer to [Search Predicates and Data Types](../../interactions/search-predicates.md) for a list of predicates
supported by a particular indexing backend.

!!! note
    Unlike composite indexes, mixed indexes do not support uniqueness.

#### Adding Property Keys

Property keys can be added to an existing mixed index which allows
subsequent queries to include this key in the query condition.
```groovy
graph.tx().rollback()  //Never create new indexes while a transaction is active
mgmt = graph.openManagement()
location = mgmt.makePropertyKey('location').dataType(Geoshape.class).make()
nameAndAge = mgmt.getGraphIndex('nameAndAge')
mgmt.addIndexKey(nameAndAge, location)
mgmt.commit()
//Previously created property keys already have the status ENABLED, but
//our newly created property key "location" needs to REGISTER so we wait for both statuses
ManagementSystem.awaitGraphIndexStatus(graph, 'nameAndAge').status(SchemaStatus.REGISTERED, SchemaStatus.ENABLED).call()
//Reindex the existing data
mgmt = graph.openManagement()
mgmt.updateIndex(mgmt.getGraphIndex("nameAndAge"), SchemaAction.REINDEX).get()
mgmt.commit()
```

To add a newly defined key, we first retrieve the existing index from
the management transaction by its name and then invoke the `addIndexKey`
method to add the key to this index.

If the added key is defined in the same management transaction, it will
be immediately available for querying. If the property key has already
been in use, adding the key requires the execution of a [reindex procedure](./index-reindexing.md) to ensure that the index contains all previously
added elements. Until the reindex procedure has completed, the key will
not be available in the mixed index.

#### Mapping Parameters

When adding a property key to a mixed index - either through the index
builder or the `addIndexKey` method - a list of parameters can be
optionally specified to adjust how the property value is mapped into the
indexing backend. Refer to the [mapping parameters
overview](../../index-backend/text-search.md) for a complete list of parameter types supported
by each indexing backend.

### Ordering

The order in which the results of a graph query are returned can be
defined using the `order().by()` directive. The `order().by()` method
expects two parameters:

-   The name of the property key by which to order the results. The
    results will be ordered by the value of the vertices or edges for
    this property key.

-   The sort order: either ascending `asc` or descending `desc`

For example, the query
`g.V().has('name', textContains('hercules')).order().by('age', desc).limit(10)`
retrieves the ten oldest individuals with *hercules* in their name.

When using `order().by()` it is important to note that:

-   Composite graph indexes do not natively support ordering search
    results. All results will be retrieved and then sorted in-memory.
    For large result sets, this can be very expensive.

-   Mixed indexes support ordering natively and efficiently. However,
    the property key used in the order().by() method must have been
    previously added to the mixed indexed for native result ordering
    support. This is important in cases where the the order().by() key
    is different from the query keys. If the property key is not part of
    the index, then sorting requires loading all results into memory.

### Label Constraint

In many cases it is desirable to only index vertices or edges with a
particular label. For instance, one may want to index only gods by their
name and not every single vertex that has a name property. When defining
an index it is possible to restrict the index to a particular vertex or
edge label using the `indexOnly` method of the index builder. The
following creates a composite index for the property key `name` that
indexes only vertices labeled `god`.
```groovy
graph.tx().rollback()  //Never create new indexes while a transaction is active
mgmt = graph.openManagement()
name = mgmt.getPropertyKey('name')
god = mgmt.getVertexLabel('god')
mgmt.buildIndex('byNameAndLabel', Vertex.class).addKey(name).indexOnly(god).buildCompositeIndex()
mgmt.commit()
//Wait for the index to become available
ManagementSystem.awaitGraphIndexStatus(graph, 'byNameAndLabel').call()
//You can check the indexOnly constraint built just now
mgmt = graph.openManagement()
mgmt.getIndexOnlyConstraint("byNameAndLabel")
//Reindex the existing data
mgmt.updateIndex(mgmt.getGraphIndex("byNameAndLabel"), SchemaAction.REINDEX).get()
mgmt.commit()
```

Label restrictions similarly apply to mixed indexes. When a composite
index with label restriction is defined as unique, the uniqueness
constraint only applies to properties on vertices or edges for the
specified label.

### Composite versus Mixed Indexes

1.  Use a composite index for exact match index retrievals. Composite
    indexes do not require configuring or operating an external index
    system and are often significantly faster than mixed indexes.

    1.  As an exception, use a mixed index for exact matches when the
        number of distinct values for query constraint is relatively
        small or if one value is expected to be associated with many
        elements in the graph (i.e. in case of low selectivity).

2.  Use a mixed indexes for numeric range, full-text or geo-spatial
    indexing. Also, using a mixed index can speed up the order().by()
    queries.

## Vertex-centric Indexes

Vertex-centric indexes, also known as Relation indexes, are local index
structures built individually per vertex. They are stored together with edges
and properties in `edgeStore`. There are two types of vertex-centric indexes,
edge indexes and property indexes.

### Edge Indexes

In large graphs vertices can have thousands of incident edges.
Traversing through those vertices can be very slow because a large
subset of the incident edges has to be retrieved and then filtered in
memory to match the conditions of the traversal. Vertex-centric indexes
can speed up such traversals by using localized index structures to
retrieve only those edges that need to be traversed.

Suppose that Hercules battled hundreds of monsters in addition to the
three captured in the introductory [Graph of the Gods](../../getting-started/basic-usage.md). Without a vertex-centric index, a query asking
for those monsters battled between time point `10` and `20` would
require retrieving all `battled` edges even though there are only a
handful of matching edges.
```groovy
h = g.V().has('name', 'hercules').next()
g.V(h).outE('battled').has('time', inside(10, 20)).inV()
```

Building a vertex-centric index by time speeds up such traversal
queries. Note, this initial index example already exists in the *Graph
of the Gods* as an index named `edges`. As a result, running the steps
below will result in a uniqueness constraint error.
```groovy
graph.tx().rollback()  //Never create new indexes while a transaction is active
mgmt = graph.openManagement()
time = mgmt.getPropertyKey('time')
battled = mgmt.getEdgeLabel('battled')
mgmt.buildEdgeIndex(battled, 'battlesByTime', Direction.BOTH, Order.desc, time)
mgmt.commit()
//Wait for the index to become available
ManagementSystem.awaitRelationIndexStatus(graph, 'battlesByTime', 'battled').call()
//Reindex the existing data
mgmt = graph.openManagement()
mgmt.updateIndex(mgmt.getRelationIndex(battled, "battlesByTime"), SchemaAction.REINDEX).get()
mgmt.commit()
```

This example builds a vertex-centric index which indexes `battled` edges
in both direction by time in descending order. A vertex-centric index is
built against a particular edge label which is the first argument to the
index construction method `JanusGraphManagement.buildEdgeIndex()`. The
index only applies to edges of this label - `battled` in the example
above. The second argument is a unique name for the index. The third
argument is the edge direction in which the index is built. The index
will only apply to traversals along edges in this direction. In this
example, the vertex-centric index is built in both direction which means
that time restricted traversals along `battled` edges can be served by
this index in both the `IN` and `OUT` direction. JanusGraph will
maintain a vertex-centric index on both the in- and out-vertex of
`battled` edges. Alternatively, one could define the index to apply to
the `OUT` direction only which would speed up traversals from Hercules
to the monsters but not in the reverse direction. This would only
require maintaining one index and hence half the index maintenance and
storage cost. The last two arguments are the sort order of the index and
a list of property keys to index by. The sort order is optional and
defaults to ascending order (i.e. `Order.ASC`). The list of property
keys must be non-empty and defines the keys by which to index the edges
of the given label. A vertex-centric index can be defined with multiple
keys.
```groovy
graph.tx().rollback()  //Never create new indexes while a transaction is active
mgmt = graph.openManagement()
time = mgmt.getPropertyKey('time')
rating = mgmt.makePropertyKey('rating').dataType(Double.class).make()
battled = mgmt.getEdgeLabel('battled')
mgmt.buildEdgeIndex(battled, 'battlesByRatingAndTime', Direction.OUT, Order.desc, rating, time)
mgmt.commit()
//Wait for the index to become available
ManagementSystem.awaitRelationIndexStatus(graph, 'battlesByRatingAndTime', 'battled').call()
//Reindex the existing data
mgmt = graph.openManagement()
mgmt.updateIndex(mgmt.getRelationIndex(battled, 'battlesByRatingAndTime'), SchemaAction.REINDEX).get()
mgmt.commit()
```

This example extends the schema by a `rating` property on `battled`
edges and builds a vertex-centric index which indexes `battled` edges in
the out-going direction by rating and time in descending order. Note,
that the order in which the property keys are specified is important
because vertex-centric indexes are prefix indexes. This means, that
`battled` edges are indexed by `rating` *first* and `time` *second*.
```groovy
h = g.V().has('name', 'hercules').next()
g.V(h).outE('battled').property('rating', 5.0) //Add some rating properties
g.V(h).outE('battled').has('rating', gt(3.0)).inV()
g.V(h).outE('battled').has('rating', 5.0).has('time', inside(10, 50)).inV()
g.V(h).outE('battled').has('time', inside(10, 50)).inV()
```

Hence, the `battlesByRatingAndTime` index can speed up the first two but
not the third query.

### Property Indexes

Similar to edge indexes, property indexes can be built to traverse properties
based on associated meta-properties efficiently. The following toy example
illustrates the usage of property indexes.

```groovy
graph = JanusGraphFactory.open("inmemory")
mgmt = graph.openManagement()
timestamp = mgmt.makePropertyKey("timestamp").dataType(Integer.class).make()
amount = mgmt.makePropertyKey("amount").dataType(Integer.class).cardinality(Cardinality.LIST).make()
mgmt.buildPropertyIndex(amount, 'amountByTime', Order.desc, timestamp)
mgmt.commit()

bob = graph.addVertex()
bob.property("amount", 100, "timestamp", 1600000000)
bob.property("amount", 200, "timestamp", 1500000000)
bob.property("amount", -150, "timestamp", 1550000000)
graph.tx().commit()

g = graph.traversal()
g.V(bob).properties("amount").has("timestamp", P.gt(1500000000))
```

In the above example, we model a number of transactions Bob has made. `amount` is a vertex
property that records the amount of money involved in each transaction, while `timestamp` is
a meta property of `amount` which records the time that a particular record happens. By creating
a property index, we can efficiently retrieve transaction amounts by timestamp. Alternatively,
we can also model the transactions as edges, then both `amount` and `timestamp` will be edge
properties, and we could use Edge indexes to speed up the query.

Multiple vertex-centric indexes can be built for the same edge label in
order to support different constraint traversals. JanusGraph’s query
optimizer attempts to pick the most efficient index for any given
traversal. Vertex-centric indexes only support equality and
range/interval constraints.

!!! note
    The property keys used in a vertex-centric index must have an
    explicitly defined data type (i.e. *not* `Object.class`) which
    supports a native sort order. This means not only that they must implement `Comparable` 
    but that their serializer must implement `OrderPreservingSerializer`. 
    The types that are currently supported are `Boolean`, `UUID`, `Byte`, `Float`, `Long`, `String`, 
    `Integer`, `Date`, `Double`, `Character`, and `Short`

If the vertex-centric index is built against either an edge label or at least one
property key that is defined in the same management transaction, the index will be
immediately available for querying. If both the edge label and all of the indexed
property keys have already been in use, building a vertex-centric index against it requires the
execution of a [reindex procedure](./index-reindexing.md) to ensure that the index
contains all previously added edges. Until the reindex procedure has
completed, the index will not be available.

!!! note
    JanusGraph automatically builds vertex-centric indexes per edge label
    and property key. That means, even with thousands of incident
    `battled` edges, queries like `g.V(h).out('mother')` or
    `g.V(h).values('age')` are efficiently answered by the local index.

Vertex-centric indexes cannot speed up unconstrained traversals which
require traversing through all incident edges of a particular label.
Those traversals will become slower as the number of incident edges
increases. Often, such traversals can be rewritten as constrained
traversals that can utilize a vertex-centric index to ensure acceptable
performance at scale.

### Using vertex-centric indexes on adjacent vertex ids

In some cases it is relevant to find an edge based on properties of the adjacent vertex.
Let's say we want to find out whether or not Hercules has battled Cerberus.
```groovy
h = g.V().has('name', 'hercules').next()
g.V(h).out('battled').has('name', 'cerberus').hasNext()
```

A query like this can not use a vertex centric index because it filters
on vertex properties rather than edge properties.
But by restructuring the query, we can achieve exactly this.
As both vertices are known, the vertex ids can be used to select the edge.
```groovy
h = g.V().has('name', 'hercules').next()
c = g.V().has('name', 'cerberus').next()
```

In contrast to the name "Cebereus", which is a property of the adjacent vertex,
the id of this vertex is already saved within the connecting edge itself.
Therefore, this query runs much faster if hercules has battled many opponents:
```groovy
g.V(h).outE('battled').where(inV().is(c)).hasNext()
```

... or even shorter:
```groovy
g.V(h).out('battled').is(c).limit(1).hasNext()
```

Assuming there is a global index on the `name` property, this improves the performance
a lot, because it's not necessary to fetch every adjacent vertex anymore.
In addition, a vertex-centric index on adjacent vertex ids does not need to
be constructed and maintained explicitly.
Due to the [data model](../../advanced-topics/data-model.md), efficient access to
edges by their adjacent vertex id is already provided by the storage backend.


### Ordered Traversals

The following queries specify an order in which the incident edges are
to be traversed. Use the `localLimit` command to retrieve a subset of
the edges (in a given order) for EACH vertex that is traversed.
```groovy
h = g.V().has('name', 'hercules').next()
g.V(h).local(outE('battled').order().by('time', desc).limit(10)).inV().values('name')
g.V(h).local(outE('battled').has('rating', 5.0).order().by('time', desc).limit(10)).values('place')
```

The first query asks for the names of the 10 most recently battled
monsters by Hercules. The second query asks for the places of the 10
most recent battles of Hercules that are rated 5 stars. In both cases,
the query is constrained by an order on a property key with a limit on
the number of elements to be returned.

Such queries can also be efficiently answered by vertex-centric indexes
if the order key matches the key of the index and the requested order
(i.e. ascending or descending) is the same as the one defined for the
index. The `battlesByTime` index would be used to answer the first query
and `battlesByRatingAndTime` applies to the second. Note, that the
`battlesByRatingAndTime` index cannot be used to answer the first query
because an equality constraint on `rating` must be present for the
second key in the index to be effective.
