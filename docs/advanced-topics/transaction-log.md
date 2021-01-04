# Transaction Log

JanusGraph can automatically log transactional changes for additional
processing or as a record of change. To enable logging for a particular
transaction, specify the name of the target log during the start of the
transaction.
```groovy
tx = graph.buildTransaction().logIdentifier('addedPerson').start()
u = tx.addVertex(label, 'human')
u.property('name', 'proteros')
u.property('age', 36)
tx.commit()
```

Upon commit, any changes made during the transaction are logged to the
user logging system into a log named `addedPerson`. The **user logging
system** is a configurable logging backend with a JanusGraph compatible
log interface. By default, the log is written to a separate store in the
primary storage backend which can be configured as described below. The
log identifier specified during the start of the transaction identifies
the log in which the changes are recorded thereby allowing different
types of changes to be recorded in separate logs for individual
processing.
```groovy
tx = graph.buildTransaction().logIdentifier('battle').start()
h = tx.traversal().V().has('name', 'hercules').next()
m = tx.addVertex(label, 'monster')
m.property('name', 'phylatax')
h.addEdge('battled', m, 'time', 22)
tx.commit()
```

JanusGraph provides a user transaction log processor framework to
process the recorded transactional changes. The transaction log
processor is opened via
`JanusGraphFactory.openTransactionLog(JanusGraph)` against a previously
opened JanusGraph graph instance. One can then add processors for a
particular log which holds transactional changes.
```groovy
import java.util.concurrent.atomic.*;
import org.janusgraph.core.log.*;
import java.util.concurrent.*;
logProcessor = JanusGraphFactory.openTransactionLog(g);
totalHumansAdded = new AtomicInteger(0);
totalGodsAdded = new AtomicInteger(0);
logProcessor.addLogProcessor("addedPerson").
    setProcessorIdentifier("addedPersonCounter").
    setStartTimeNow().
    addProcessor(new ChangeProcessor() {
        @Override
        public void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState) {
            for (v in changeState.getVertices(Change.ADDED)) {
                if (v.label().equals("human")) totalHumansAdded.incrementAndGet();
            }
        }
    }).
    addProcessor(new ChangeProcessor() {
        @Override
        public void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState) {
            for (v in changeState.getVertices(Change.ADDED)) {
                if (v.label().equals("god")) totalGodsAdded.incrementAndGet();
            }
        }
    }).
    build();
```

In this example, a **log processor** is built for the user transaction
log named `addedPerson` to process the changes made in transactions
which used the `addedPerson` log identifier. Two **change processors**
are added to this log processor. The first processor counts the number
of humans added and the second counts the number of gods added to the
graph.

When a log processor is built against a particular log, such as the
`addedPerson` log in the example above, it will start reading
transactional change records from the log immediately upon successful
construction and initialization up to the head of the log. The start
time specified in the builder marks the time point in the log where the
log processor will start reading records. Optionally, one can specify an
identifier for the log processor in the builder. The log processor will
use the identifier to regularly persist its state of processing, i.e. it
will maintain a marker on the last read log record. If the log processor
is later restarted with the same identifier, it will continue reading
from the last read record. This is particularly useful when the log
processor is supposed to run for long periods of time and is therefore
likely to fail. In such failure situations, the log processor can simply
be restarted with the same identifier. It must be ensured that log
processor identifiers are unique in a JanusGraph cluster in order to
avoid conflicts on the persisted read markers.

A change processor must implement the `ChangeProcessor` interface. It’s
`process()` method is invoked for each change record read from the log
with a `JanusGraphTransaction` handle, the id of the transaction that
caused the change, and a `ChangeState` container which holds the
transactional changes. The change state container can be queried to
retrieve individual elements that were part of the change state. In the
example, all added vertices are retrieved. Refer to the API
documentation for a description of all the query methods on
`ChangeState`. The provided transaction id can be used to investigate
the origin of the transaction which is uniquely identified by the
combination of the id of the JanusGraph instance that executed the
transaction (`txId.getInstanceId()`) and the instance specific
transaction id (`txId.getTransactionId()`). In addition, the time of the
transaction is available through `txId.getTransactionTime()`.

Change processors are executed individually and in multiple threads. If
a change processor accesses global state it must be ensured that such
state allows concurrent access. While the log processor reads log
records sequentially, the changes are processed in multiple threads so
it cannot be guaranteed that the log order is preserved in the change
processors.

Note, that log processors run each registered change processor at least
once for each record in the log which means that a single transactional
change record may be processed multiple times under certain failure
conditions. One cannot add or remove change processor from a running log
processor. In other words, a log processor is immutable after it is
built. To change log processing, start a new log processor and shut down
an existing one.
```groovy
logProcessor.addLogProcessor("battle").
    setProcessorIdentifier("battleTimer").
    setStartTimeNow().
    addProcessor(new ChangeProcessor() {
        @Override
        public void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState) {
            h = tx.V().has("name", "hercules").toList().iterator().next();
            for (edge in changeState.getEdges(h, Change.ADDED, Direction.OUT, "battled")) {
                if (edge.<Integer>value("time")>1000)
                    h.property("oldFighter", true);
            }
        }
    }).
    build();
```

The log processor above processes transactions for the `battle` log
identifier with a single change processor which evaluates `battled`
edges that were added to Hercules. This example demonstrates that the
transaction handle passed into the change processor is a normal
`JanusGraphTransaction` which query the JanusGraph graph and make
changes to it.

## Transaction Log Use Cases

### Record of Change

The user transaction log can be used to keep a record of all changes
made against the graph. By using separate log identifiers, changes can
be recorded in different logs to distinguish separate transaction types.

At any time, a log processor can be built which can processes all
recorded changes starting from the desired start time. This can be used
for forensic analysis, to replay changes against a different graph, or
to compute an aggregate.

### Downstream Updates

It is often the case that a JanusGraph graph cluster is part of a larger
architecture. The user transaction log and the log processor framework
provide the tools needed to broadcast changes to other components of the
overall system without slowing down the original transactions causing
the change. This is particularly useful when transaction latencies need
to be low and/or there are a number of other systems that need to be
alerted to a change in the graph.

### Triggers

The user transaction log provides the basic infrastructure to implement
triggers that can scale to a large number of concurrent transactions and
very large graphs. A trigger is registered with a particular change of
data and either triggers an event in an external system or additional
changes to the graph. At scale, it is not advisable to implement
triggers in the original transaction but rather process triggers with a
slight delay through the log processor framework. The second example
shows how changes to the graph can be evaluated and trigger additional
modifications.

## Log Configuration

There are a number of configuration options to fine tune how the log
processor reads from the log. Refer to the complete list of
configuration options [Configuration Reference](../configs/configuration-reference.md) for the
options under the `log` namespace. To configure the user transaction
log, use the `log.user` namespace. The options listed there allow the
configuration of the number of threads to be used, the number of log
records read in each batch, the read interval, and whether the
transaction change records should automatically expire and be removed
from the log after a configurable amount of time (TTL).

