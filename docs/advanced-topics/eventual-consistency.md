# Eventually-Consistent Storage Backends

When running JanusGraph against an eventually consistent storage backend
special JanusGraph features must be used to ensure data consistency and
special considerations must be made regarding data degradation.

This page summarizes some of the aspects to consider when running
JanusGraph on top of an eventually consistent storage backend like
Apache Cassandra or Apache HBase.

## Data Consistency

On eventually consistent storage backends, JanusGraph must obtain locks
in order to ensure consistency because the underlying storage backend
does not provide transactional isolation. In the interest of efficiency,
JanusGraph does not use locking by default. Hence, the user has to
decide for each schema element that defines a consistency constraint
whether or not to use locking. Use `JanusGraphManagement.setConsistency(element, ConsistencyModifier.LOCK)`
to explicitly enable locking on a schema element as shown in the
following examples.
```groovy
mgmt = graph.openManagement()
name = mgmt.makePropertyKey('consistentName').dataType(String.class).make()
index = mgmt.buildIndex('byConsistentName', Vertex.class).addKey(name).unique().buildCompositeIndex()
mgmt.setConsistency(name, ConsistencyModifier.LOCK) // Ensures only one name per vertex
mgmt.setConsistency(index, ConsistencyModifier.LOCK) // Ensures name uniqueness in the graph
mgmt.commit()
```

When updating an element that is guarded by a uniqueness constraint,
JanusGraph uses the following protocol at the end of a transaction when
calling `tx.commit()`:

1.  Acquire a lock on all elements that have a consistency constraint
2.  Re-read those elements from the storage backend and verify that they
    match the state of the element in the current transaction prior to
    modification. If not, the element was concurrently modified and a
    PermanentLocking exception is thrown.
3.  Persist the state of the transaction against the storage backend.
4.  Release all locks.

This is a brief description of the locking protocol which leaves out
optimizations (e.g. local conflict detection) and detection of failure
scenarios (e.g. expired locks).

The actual lock application mechanism is abstracted such that JanusGraph
can use multiple implementations of a locking provider. Currently, only one
locking provider is included in the JanusGraph distribution:

A locking implementation based on key-consistent read and write
operations that is agnostic to the underlying storage backend as
long as it supports key-consistent operations (which includes
Cassandra and HBase). This is the default implementation and uses
timestamp based lock applications to determine which transaction
holds the lock. It requires that clocks are synchronized across all
machines in the cluster.

!!! warning
    The locking implementation is not robust against all failure
    scenarios. For instance, when a Cassandra cluster drops below quorum,
    consistency is no longer ensured. Hence, it is suggested to use
    locking-based consistency constraints sparingly with eventually
    consistent storage backends. For use cases that require strict and or
    frequent consistency constraint enforcement, it is suggested to use a
    storage backend that provides transactional isolation.

### Data Consistency without Locks

Because of the additional steps required to acquire a lock when
committing a modifying transaction, locking is a fairly expensive way to
ensure consistency and can lead to deadlock when very many concurrent
transactions try to modify the same elements in the graph. Hence,
locking should be used in situations where consistency is more important
than write latency and the number of conflicting transactions is small.

In other situations, it may be better to allow conflicting transactions
to proceed and to resolve inconsistencies at read time. This is a design
pattern commonly employed in large scale data systems and most effective
when the actual likelihood of conflict is small. Hence, write
transactions don’t incur additional overhead and any (unlikely) conflict
that does occur is detected and resolved at read time and later cleaned
up. JanusGraph makes it easy to use this strategy through the following
features.

#### Forking Edges

Because edge are stored as single records in the underlying storage
backend, concurrently modifying a single edge would lead to conflict.
Instead of locking, an edge label can be configured to use
`ConsistencyModifier.FORK`. The following example creates a new edge
label `related` and defines its consistency to FORK.

```groovy
mgmt = graph.openManagement()
related = mgmt.makeEdgeLabel('related').make()
mgmt.setConsistency(related, ConsistencyModifier.FORK)
mgmt.commit()
```

When modifying an edge whose label is configured to FORK the edge is
deleted and the modified edge is added as a new one. Hence, if two
concurrent transactions modify the same edge, two modified copies of the
edge will exist upon commit which can be resolved during querying
traversals if needed.

!!! note
    Edge forking only applies to MULTI edges. Edge labels with a
    multiplicity constraint cannot use this strategy since a constraint is
    built into the edge label definition that requires an explicit lock or
    use the conflict resolution mechanism of the underlying storage
    backend.

#### Multi-Properties

Modifying single valued properties on vertices concurrently can result
in a conflict. Similarly to edges, one can allow an arbitrary number of
properties on a vertex for a particular property key defined with
cardinality LIST and FORK on modification. Hence, instead of conflict
one reads multiple properties. Since JanusGraph allows properties on
properties, provenance information like `author` can be added to the
properties to facilitate resolution at read time.

See [multi-properties](../schema/index.md#property-key-cardinality) to learn how to define
those.

## Data Inconsistency

### Temporary Inconsistency

On eventually consistent storage backends, writes may not be immediately
visible to the entire cluster causing temporary inconsistencies in the
graph. This is an inherent property of eventual consistency, in the
sense, that accepted updates must be propagated to other instances in
the cluster and no guarantees are made with respect to read atomicity in
the interest of performance.

From JanusGraph’s perspective, eventual consistency might cause the
following temporary graph inconsistencies in addition the general
inconsistency that some parts of a transaction are visible while others
aren’t yet.

**Stale Index entries**  
Index entries might point to nonexistent vertices or edges. Similarly, a
vertex or edge appears in the graph but is not yet indexed and hence
ignored by global graph queries.

**Half-Edges**  
Only one direction of an edge gets persisted or deleted which might lead
to the edge not being or incorrectly being retrieved.

!!! note
    In order to avoid that write failures result in permanent
    inconsistencies in the graph it is recommended to use storage backends
    that support batch write atomicity and to ensure that write atomicity
    is enabled. To get the benefit of write atomicity, the number
    modifications made in a single transaction must be smaller than the
    configured `buffer-size` option documented in [Configuration Reference](../configs/configuration-reference.md). The
    buffer size defines the maximum number of modifications that
    JanusGraph will persist in a single batch. If a transaction has more
    modifications, the persistence will be split into multiple batches
    which are persisted individually which is useful for batch loading but
    invalidates write atomicity.

### Ghost Vertices

A permanent inconsistency that can arise when operating JanusGraph on
eventually consistent storage backend is the phenomena of **ghost
vertices**. If a vertex gets deleted while it is concurrently being
modified, the vertex might re-appear as a *ghost*.

The following strategies can be used to mitigate this issue:

**Existence checks**  
Configure transactions to (double) check for the existence of vertices
prior to returning them. Please see [Transaction Configuration](../interactions/transactions.md#transaction-configuration) for more
information and note that this can significantly decrease performance.
Note, that this does not fix the inconsistencies but hides some of them
from the user.

**Regular Clean-ups**  
Run regular batch-jobs to repair inconsistencies in the graph using
[JanusGraph with TinkerPop’s Hadoop-Gremlin](hadoop.md). 
This is the only strategy that can address all
inconsistencies and effectively repair them.

**Soft Deletes**  
Instead of deleting vertices, they are marked as deleted which keeps
them in the graph for future analysis but hides them from user-facing
transactions.
