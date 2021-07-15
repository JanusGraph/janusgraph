# Transactions

Almost all interaction with JanusGraph is associated with a transaction.
JanusGraph transactions are safe for concurrent use by multiple threads.
Methods on a JanusGraph instance like `graph.V(...)` and
`graph.tx().commit()` perform a `ThreadLocal` lookup to retrieve or
create a transaction associated with the calling thread. Callers can
alternatively forego `ThreadLocal` transaction management in favor of
calling `graph.tx().createThreadedTx()`, which returns a reference to a
transaction object with methods to read/write graph data and commit or
rollback.

JanusGraph transactions are not necessarily ACID. They can be so
configured on BerkeleyDB, but they are not generally so on Cassandra or
HBase, where the underlying storage system does not provide serializable
isolation or multi-row atomic writes and the cost of simulating those
properties would be substantial.

This section describes JanusGraph’s transactional semantics and API.

## Transaction Handling

Every graph operation in JanusGraph occurs within the context of a
transaction. According to the TinkerPop’s transactional specification,
each thread opens its own transaction against the graph database with
the first operation (i.e. retrieval or mutation) on the graph:
```groovy
graph = JanusGraphFactory.open("berkeleyje:/tmp/janusgraph")
juno = graph.addVertex() //Automatically opens a new transaction
juno.property("name", "juno")
graph.tx().commit() //Commits transaction
```

In this example, a local JanusGraph graph database is opened. Adding the
vertex "juno" is the first operation (in this thread) which
automatically opens a new transaction. All subsequent operations occur
in the context of that same transaction until the transaction is
explicitly stopped or the graph database is closed. If transactions are
still open when `close()` is called, then the behavior of the
outstanding transactions is technically undefined. In practice, any
non-thread-bound transactions will usually be effectively rolled back,
but the thread-bound transaction belonging to the thread that invoked
shutdown will first be committed. Note, that both read and write
operations occur within the context of a transaction.

## Transactional Scope

All graph elements (vertices, edges, and types) are associated with the
transactional scope in which they were retrieved or created. Under
TinkerPop’s default transactional semantics, transactions are
automatically created with the first operation on the graph and closed
explicitly using `commit()` or `rollback()`. Once the transaction is
closed, all graph elements associated with that transaction become stale
and unavailable. However, JanusGraph will automatically transition
vertices and types into the new transactional scope as shown in this
example:
```groovy
graph = JanusGraphFactory.open("berkeleyje:/tmp/janusgraph")
juno = graph.addVertex() //Automatically opens a new transaction
graph.tx().commit() //Ends transaction
juno.property("name", "juno") //Vertex is automatically transitioned
```

Edges, on the other hand, are not automatically transitioned and cannot
be accessed outside their original transaction. They must be explicitly
transitioned:
```groovy
e = juno.addEdge("knows", graph.addVertex())
graph.tx().commit() //Ends transaction
e = g.E(e).next() //Need to refresh edge
e.property("time", 99)
```

## Transaction Failures

When committing a transaction, JanusGraph will attempt to persist all
changes to the storage backend. This might not always be successful due
to IO exceptions, network errors, machine crashes or resource
unavailability. Hence, transactions can fail. In fact, transactions
**will eventually fail** in sufficiently large systems. Therefore, we
highly recommend that your code expects and accommodates such failures:
```groovy
try {
    if (g.V().has("name", name).iterator().hasNext())
        throw new IllegalArgumentException("Username already taken: " + name)
    user = graph.addVertex()
    user.property("name", name)
    graph.tx().commit()
} catch (Exception e) {
    //Recover, retry, or return error message
    println(e.getMessage())
}
```

The example above demonstrates a simplified user signup implementation
where `name` is the name of the user who wishes to register. First, it
is checked whether a user with that name already exists. If not, a new
user vertex is created and the name assigned. Finally, the transaction
is committed.

If the transaction fails, a `JanusGraphException` is thrown. There are a
variety of reasons why a transaction may fail. JanusGraph differentiates
between *potentially temporary* and *permanent* failures.

Potentially temporary failures are those related to resource
unavailability and IO hiccups (e.g. network timeouts). JanusGraph
automatically tries to recover from temporary failures by retrying to
persist the transactional state after some delay. The number of retry
attempts and the retry delay are configurable (see [Configuration Reference](../configs/configuration-reference.md)).

Permanent failures can be caused by complete connection loss, hardware
failure or lock contention. To understand the cause of lock contention,
consider the signup example above and suppose a user tries to signup
with username "juno". That username may still be available at the
beginning of the transaction but by the time the transaction is
committed, another user might have concurrently registered with "juno"
as well and that transaction holds the lock on the username therefore
causing the other transaction to fail. Depending on the transaction
semantics one can recover from a lock contention failure by re-running
the entire transaction.

Permanent exceptions that can fail a transaction include:

-   PermanentLockingException(**Local lock contention**): Another local
    thread has already been granted a conflicting lock.

-   PermanentLockingException(**Expected value mismatch for X:
    expected=Y vs actual=Z**): The verification that the value read in
    this transaction is the same as the one in the datastore after
    applying for the lock failed. In other words, another transaction
    modified the value after it had been read and modified.

## Multi-Threaded Transactions

JanusGraph supports multi-threaded transactions through TinkerPop’s threaded transactions.
 Hence, to speed up transaction processing and utilize multi-core architectures multiple threads can run concurrently in a single transaction.

With TinkerPop’s default transaction handling, each thread automatically
opens its own transaction against the graph database. To open a
thread-independent transaction, use the `createThreadedTx()` method.
```groovy
threadedGraph = graph.tx().createThreadedTx();
threads = new Thread[10];
for (int i=0; i<threads.length; i++) {
    threads[i]=new Thread({
        println("Do something with 'threadedGraph''");
    });
    threads[i].start();
}
for (int i=0; i<threads.length; i++) threads[i].join();
threadedGraph.tx().commit();
```

The `createThreadedTx()` method returns a new `Graph` object that
represents this newly opened transaction. The graph object `tx` supports
all of the methods that the original graph did, but does so without
opening new transactions for each thread. This allows us to start
multiple threads which all work concurrently in the same transaction and
one of which finally commits the transaction when all threads have
completed their work.

JanusGraph relies on optimized concurrent data structures to support
hundreds of concurrent threads running efficiently in a single
transaction.

## Concurrent Algorithms

Thread independent transactions started through `createThreadedTx()` are
particularly useful when implementing concurrent graph algorithms. Most
traversal or message-passing (ego-centric) like graph algorithms are
[embarrassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) which
means they can be parallelized and executed through multiple threads
with little effort. Each of these threads can operate on a single
`Graph` object returned by `createThreadedTx()` without blocking each
other.

## Nested Transactions

Another use case for thread independent transactions is nested
transactions that ought to be independent from the surrounding
transaction.

For instance, assume a long running transactional job that has to create
a new vertex with a unique name. Since enforcing unique names requires
the acquisition of a lock (see [Eventually-Consistent Storage Backends](../advanced-topics/eventual-consistency.md) for more
detail) and since the transaction is running for a long time, lock
congestion and expensive transactional failures are likely.
```groovy
v1 = graph.addVertex()
//Do many other things
v2 = graph.addVertex()
v2.property("uniqueName", "foo")
v1.addEdge("related", v2)
//Do many other things
graph.tx().commit() // This long-running tx might fail due to contention on its uniqueName lock
```

One way around this is to create the vertex in a short, nested thread-independent transaction as demonstrated by the following pseudo code  

```groovy
v1 = graph.addVertex()
//Do many other things
tx = graph.tx().createThreadedTx()
v2 = tx.addVertex()
v2.property("uniqueName", "foo")
tx.commit() // Any lock contention will be detected here
v1.addEdge("related", g.V(v2).next()) // Need to load v2 into outer transaction
//Do many other things
graph.tx().commit() // Can't fail due to uniqueName write lock contention involving v2
```

## Common Transaction Handling Problems

Transactions are started automatically with the first operation executed
against the graph. One does NOT have to start a transaction manually.
The method `newTransaction` is used to start [multi-threaded transactions](#multi-threaded-transactions) only.

Transactions are automatically started under the TinkerPop semantics but
**not** automatically terminated. Transactions must be terminated
manually with `commit()` or `rollback()`. If a `commit()` transactions
fails, it should be terminated manually with `rollback()` after catching
the failure. Manual termination of transactions is necessary because
only the user knows the transactional boundary.

A transaction will attempt to maintain its state from the beginning of the transaction. This might lead to unexpected behavior in multi-threaded applications as illustrated in the following artificial example  

```groovy
v = g.V(4).next() // Retrieve vertex, first action automatically starts transaction
g.V(v).bothE()
>> returns nothing, v has no edges
//thread is idle for a few seconds, another thread adds edges to v
g.V(v).bothE()
>> still returns nothing because the transactional state from the beginning is maintained
```

Such unexpected behavior is likely to occur in client-server
applications where the server maintains multiple threads to answer
client requests. It is therefore important to terminate the transaction
after a unit of work (e.g. code snippet, query, etc). So, the example
above should be:

```groovy
v = g.V(4).next() // Retrieve vertex, first action automatically starts transaction
g.V(v).bothE()
graph.tx().commit()
//thread is idle for a few seconds, another thread adds edges to v
g.V(v).bothE()
>> returns the newly added edge
graph.tx().commit()
```

When using multi-threaded transactions via `newTransaction` all vertices
and edges retrieved or created in the scope of that transaction are
**not** available outside the scope of that transaction. Accessing such
elements after the transaction has been closed will result in an
exception. As demonstrated in the example above, such elements have to
be explicitly refreshed in the new transaction using
`g.V(existingVertex)` or `g.E(existingEdge)`.

## Transaction Configuration

JanusGraph’s `JanusGraph.buildTransaction()` method gives the user the
ability to configure and start a new [multi-threaded transaction](#multi-threaded-transactions) 
against a `JanusGraph`. Hence, it is identical to `JanusGraph.newTransaction()` with additional configuration
options.

`buildTransaction()` returns a `TransactionBuilder` which allows the
following aspects of a transaction to be configured:

-   `readOnly()` - makes the transaction read-only and any attempt to
    modify the graph will result in an exception.

-   `enableBatchLoading()` - enables batch-loading for an individual
    transaction. This setting results in similar efficiencies as the
    graph-wide setting `storage.batch-loading` due to the disabling of
    consistency checks and other optimizations. Unlike
    `storage.batch-loading` this option will not change the behavior of
    the storage backend. Similarly, you could call `disableBatchLoading()`
    to disable batch-loading for an individual transaction.

-   `propertyPrefetching(boolean)` - enables or disables property prefetching,
    i.e. `query.fast-property`, for an individual transaction. If enabled,
    all properties of a particular vertex will be pre-fetched on the first
    vertex property access, which eliminates backend calls on subsequent
    property access for the same vertex.

-   `multiQuery(boolean)` - enables or disables query batching, i.e.
    `query.batch`, for an individual transaction. If enabled, queries
    for a single traversal will be batched when executed against the
    storage backend.

-   `setTimestamp(long)` - Sets the timestamp for this transaction as
    communicated to the storage backend for persistence. Depending on
    the storage backend, this setting may be ignored. For eventually
    consistent backends, this is the timestamp used to resolve write
    conflicts. If this setting is not explicitly specified, JanusGraph
    uses the current time.

-   `setVertexCacheSize(long size)` - The number of vertices this
    transaction caches in memory. The larger this number, the more
    memory a transaction can potentially consume. If this number is too
    small, a transaction might have to re-fetch data which causes delays
    in particular for long running transactions.

-   `checkExternalVertexExistence(boolean)` - Whether this transaction
    should verify the existence of vertices for user provided vertex
    ids. Such checks requires access to the database which takes time.
    The existence check should only be disabled if the user is
    absolutely sure that the vertex must exist - otherwise data
    corruption can ensue.

-   `checkInternalVertexExistence(boolean)` - Whether this transaction
    should double-check the existence of vertices during query
    execution. This can be useful to avoid **phantom vertices** on
    eventually consistent storage backends. Disabled by default.
    Enabling this setting can slow down query processing.

-   `consistencyChecks(boolean)` - Whether JanusGraph should enforce
    schema level consistency constraints (e.g. multiplicity
    constraints). Disabling consistency checks leads to better
    performance but requires that the user ensures consistency
    confirmation at the application level to avoid inconsistencies. USE
    WITH GREAT CARE!

Once, the desired configuration options have been specified, the new
transaction is started via `start()` which returns a
`JanusGraphTransaction`.
