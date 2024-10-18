# Oracle Berkeley DB Java Edition

> Oracle Berkeley DB Java Edition is an open source, embeddable,
> transactional storage engine written entirely in Java. It takes full
> advantage of the Java environment to simplify development and
> deployment. The architecture of Oracle Berkeley DB Java Edition
> supports very high performance and concurrency for both read-intensive
> and write-intensive workloads.
>
> —  [Oracle Berkeley DB Java Edition
> Homepage](http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html)

The [Oracle Berkeley DB Java
Edition](http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html)
storage backend runs in the same JVM as JanusGraph and provides local
persistence on a single machine. Hence, the BerkeleyDB storage backend
requires that all of the graph data fits on the local disk and all of
the frequently accessed graph elements fit into main memory. This
imposes a practical limitation of graphs with 10-100s million vertices
on commodity hardware. However, for graphs of that size the BerkeleyDB
storage backend exhibits high performance because all data can be
accessed locally within the same JVM.

## BerkeleyDB JE Setup

Since BerkeleyDB runs in the same JVM as JanusGraph, connecting the two
only requires a simple configuration and no additional setup:
```java
JanusGraph g = JanusGraphFactory.build().
    set("storage.backend", "berkeleyje").
    set("storage.directory", "/data/graph").
open();
```

In the Gremlin Console, you can not define the type of the variables
`conf` and `g`. Therefore, simply leave off the type declaration.

## BerkeleyDB Specific Configuration

Refer to [Configuration Reference](../configs/configuration-reference.md) for a complete listing of all BerkeleyDB
specific configuration options in addition to the general JanusGraph
configuration options. BerkeleyDB configured with
[SHARED_CACHE](https://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/EnvironmentConfig.html#SHARED_CACHE)
those multiple graphs will make better use of memory because the cache LRU algorithm is applied across all
information in all graphs sharing the cache.

When configuring BerkeleyDB it is recommended to consider the following
BerkeleyDB specific configuration options:

-   **transactions**: Enables transactions and detects conflicting
    database operations. **CAUTION:** While disabling transactions can
    lead to better performance it can cause to inconsistencies and even
    corrupt the database if multiple JanusGraph instances interact with
    the same instance of BerkeleyDB.
-   **cache-percentage**: The percentage of JVM heap space (configured
    via -Xmx) to be allocated to BerkeleyDB for its cache. Try to give
    BerkeleyDB as much space as possible without causing memory problems
    for JanusGraph. For instance, if JanusGraph only runs short
    transactions, use a value of 80 or higher.

## Ideal Use Case

The BerkeleyDB storage backend is best suited for small to medium size
graphs with up to 100 million vertices on commodity hardware. For graphs
of that size, it will likely deliver higher performance than the
distributed storage backends. Note, that BerkeleyDB is also limited in
the number of concurrent requests it can handle efficiently because it
runs on a single machine. Hence, it is not well suited for applications
with many concurrent users mutating the graph, even if that graph is
small to medium size.

Since BerkeleyDB runs in the same JVM as JanusGraph, this storage
backend is ideally suited for unit testing of application code using
JanusGraph.

## Global Graph Operations

JanusGraph backed by BerkeleyDB supports global graph operations such as
iterating over all vertices or edges. However, note that such operations
need to scan the entire database which can require a significant amount
of time for larger graphs.

In order to not run out of memory, it is advised to disable transactions
(`storage.transactions=false`) when iterating over large graphs. Having
transactions enabled requires BerkeleyDB to acquire read locks on the
data it is reading. When iterating over the entire graph, these read
locks can easily require more memory than is available.

## Additional BerkeleyDB JE configuration options

It's possible to set additional BerkeleyDB JE configuration which are not 
directly exposed by JanusGraph by leveraging `storage.berkeleyje.ext` 
namespace. 

JanusGraph iterates over all properties prefixed with
`storage.berkeleyje.ext.`. It strips the prefix from each property key. 
Any dash character (`-`) wil be replaced by dot character (`.`) in the 
remainder of the stripped key. The final string will be interpreted as a parameter 
ke for `com.sleepycat.je.EnvironmentConfig`. 
Thus, both options `storage.berkeleyje.ext.je.lock.timeout` and 
`storage.berkeleyje.ext.je-lock-timeout` will be treated 
the same (as `storage.berkeleyje.ext.je.lock.timeout`). 
The value associated with the key is not modified. 
This allows embedding arbitrary settings in JanusGraph’s properties. Here’s an
example configuration fragment that customizes three BerkeleyDB settings 
using the `storage.berkeleyje.ext.` config mechanism:

```properties
storage.backend=berkeleyje
storage.berkeleyje.ext.je.lock.timeout=5000 ms
storage.berkeleyje.ext.je.lock.deadlockDetect=false
storage.berkeleyje.ext.je.txn.timeout=5000 ms
storage.berkeleyje.ext.je.log.fileMax=100000000
```

## Deadlock troubleshooting

In concurrent environment deadlocks are possible when using BerkeleyDB JE storage 
backend. 
It may be complicated to deal with deadlocks in use-cases when multiple threads are 
modifying same vertices (including edges creation between affected vertices). 
More insights on this topic can be found in the GitHub issue 
[#1623](https://github.com/JanusGraph/janusgraph/issues/1623).

Some users suggest the following configuration to deal with deadlocks:
```properties
storage.berkeleyje.isolation-level=READ_UNCOMMITTED
storage.berkeleyje.lock-mode=LockMode.READ_UNCOMMITTED
storage.berkeleyje.ext.je.lock.timeout=0
storage.lock.wait-time=5000
ids.authority.wait-time=2000
tx.max-commit-time=30000
```
