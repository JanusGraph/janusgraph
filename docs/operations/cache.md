# JanusGraph Cache
## Caching

JanusGraph employs multiple layers of data caching to facilitate fast
graph traversals. The caching layers are listed here in the order they
are accessed from within a JanusGraph transaction. The closer the cache
is to the transaction, the faster the cache access and the higher the
memory footprint and maintenance overhead.

## Transaction-Level Caching

Within an open transaction, JanusGraph maintains two caches:

-   Vertex Cache: Caches accessed vertices and their adjacency list (or
    subsets thereof) so that subsequent access is significantly faster
    within the same transaction. Hence, this cache speeds up iterative
    traversals.

-   Index Cache: Caches the results for index queries so that subsequent
    index calls can be served from memory instead of calling the index
    backend and (usually) waiting for one or more network round trips.

The size of both of those is determined by the *transaction cache size*.
The transaction cache size can be configured via `cache.tx-cache-size`
or on a per transaction basis by opening a transaction via the
transaction builder `graph.buildTransaction()` and using the
`setVertexCacheSize(int)` method.

### Vertex Cache

The vertex cache contains vertices and the subset of their adjacency
list (properties and edges) that has been retrieved in a particular transaction.
The maximum number of vertices maintained in this cache is equal to the transaction
cache size. If the transaction workload is an iterative traversal, the
vertex cache will significantly speed it up. If the same vertex is not
accessed again in the transaction, the transaction level cache will make
no difference.

Note, that the size of the vertex cache on heap is not only determined
by the number of vertices it may hold but also by the size of their
adjacency list. In other words, vertices with large adjacency lists
(i.e. many incident edges) will consume more space in this cache than
those with smaller lists.

Furthermore note, that modified vertices are *pinned* in the cache,
which means they cannot be evicted since that would entail loosing their
changes. Therefore, transaction which contain a lot of modifications may
end up with a larger than configured vertex cache.

Assuming your vertex is not evicted from cache, or it is evicted from
cache but your program context still holds the reference to the vertex,
then its properties and edges are cached together with the vertex. This means once
a property is queried, any subsequent reads will hit the cache. In case
you want to force JanusGraph to read from the data storage again (provided you
have disabled database-level cache), or you simply want to save memory, you could clear
the cache of that vertex manually. Note this operation is not gremlin compliant, so
you need to cast your vertex into CacheVertex type to do the refresh:

```groovy
// first read automatically caches the property together with v
v.property("prop").value();
// force refresh to clear the cache
((CacheVertex) v).refresh();
// now a subsequent read will look up in JanusGraph's database-level
// cache, and then backend storage read in case of cache miss
v.property("prop").value();
```

Note that refresh operation cannot guarantee your current transaction reads
the latest data from an eventual consistent backend. You should not attempt
to achieve Compare-And-Set (CAS) via refresh operation, even though it might
be helpful to detect conflicts among transactions in some cases.

### Index Cache

The index cache contains the results of index queries executed in the
context of this transaction. Subsequent identical index calls will be
served from this cache and are therefore significantly cheaper. If the
same index call never occurs twice in the same transaction, the index
cache makes no difference.

Each entry in the index cache is given a weight equal to
`2 + result set size` and the total weight of the cache will not exceed
half of the transaction cache size.

## Database Level Caching

The database level cache contains vertices and the subset of their adjacency
list (properties and edges) across multiple transactions and beyond the duration
of a single transaction. The database level cache is shared by all transactions
across a database. It is more space efficient than the transaction level
caches but also slightly slower to access. In contrast to the
transaction level caches, the database level caches do not expire
immediately after closing a transaction. Hence, the database level cache
significantly speeds up graph traversals for read heavy workloads across
transactions. A read looks up in transaction-level cache first,
and then database-level cache.

[Configuration Reference](../configs/configuration-reference.md) lists all of the configuration
options that pertain to JanusGraph’s database level cache. This page
attempts to explain their usage.

Most importantly, the database level cache is disabled by default in the
current release version of JanusGraph. To enable it, set
`cache.db-cache=true`.

### Cache Expiration Time

The most important setting for performance and query behavior is the
cache expiration time which is configured via `cache.db-cache-time`. The
cache will hold graph elements for at most that many milliseconds. If an
element expires, the data will be re-read from the storage backend on
the next access.

If there is only one JanusGraph instance accessing the storage backend
or if this instance is the only one modifying the graph, the cache
expiration can be set to 0 which disables cache expiration. This allows
the cache to hold elements indefinitely (unless they are evicted due to
space constraints or on update) which provides the best cache
performance. Since no other JanusGraph instance is modifying the graph,
there is no danger of holding on to stale data.

If there are multiple JanusGraph instances accessing the storage
backend, the time should be set to the maximum time that can be allowed
between **another** JanusGraph instance modifying the graph and this
JanusGraph instance seeing the data. If any change should be immediately
visible to all JanusGraph instances, the database level cache should be
disabled in a distributed setup. However, for most applications it is
acceptable that a particular JanusGraph instance sees remote
modifications with some delay. The larger the maximally allowed delay,
the better the cache performance. Note, that a given JanusGraph instance
will always immediately see its own modifications to the graph
irrespective of the configured cache expiration time.

### Cache Size

The configuration option `cache.db-cache-size` controls how much heap
space JanusGraph’s database level cache is allowed to consume. The
larger the cache, the more effective it will be. However, large cache
sizes can lead to excessive GC and poor performance.

The cache size can be configured as a percentage (expressed as a decimal
between 0 and 1) of the total heap space available to the JVM running
JanusGraph or as an absolute number of bytes.

Note, that the cache size refers to the amount of heap space that is
exclusively occupied by the cache. JanusGraph’s other data structures
and each open transaction will occupy additional heap space. If
additional software layers are running in the same JVM, those may occupy
a significant amount of heap space as well (e.g. Gremlin Server, etc). 
Be conservative in your heap memory estimation. Configuring a cache 
that is too large can lead to out-of-memory exceptions and excessive GC.

In practice, you might observe JanusGraph uses more memory than configured
for database level cache. This is [a known limitation](https://github.com/JanusGraph/janusgraph/issues/2369)
due to difficulty of estimating size of deserialized objects.

### Clean Up Wait Time

When a vertex is locally modified (e.g. an edge is added) all of the
vertex’s related database level cache entries are marked as expired and
eventually evicted. This will cause JanusGraph to refresh the vertex’s
data from the storage backend on the next access and re-populate the
cache.

However, when the storage backend is eventually consistent, the
modifications that triggered the eviction may not yet be visible. By
configuring `cache.db-cache-clean-wait`, the cache will wait for at
least this many milliseconds before repopulating the cache with the
entry retrieved from the storage backend.

If JanusGraph runs locally or against a storage backend that guarantees
immediate visibility of modifications, this value can be set to 0.

## Storage Backend Caching

Each storage backend maintains its own data caching layer. These caches
benefit from compression, data compactness, coordinated expiration and
are often maintained off heap which means that large caches can be used
without running into garbage collection issues. While these caches can
be significantly larger than the database level cache, they are also
slower to access.

The exact type of caching and its properties depends on the particular
[storage backend](../storage-backend/index.md). Please refer to the respective
documentation for more information about the caching infrastructure and
how to optimize it.
