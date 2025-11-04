# Failure & Recovery

JanusGraph is a highly available and robust graph database. In large
scale JanusGraph deployments failure is inevitable. This page describes
some failure situations and how JanusGraph can handle them.

## Transaction Failure

Transactions can fail for a number of reasons. If the transaction fails
before the commit the changes will be discarded and the application can
retry the transaction in coherence with the business logic. Likewise,
locking or other consistency failures will cause an exception prior to
persistence and hence can be retried. The persistence stage of a
transaction is when JanusGraph starts persisting data to the various
backend systems.

JanusGraph first persists all graph mutations to the storage backend.
This persistence is executed as one batch mutation to ensure that the
mutation is committed atomically for those backends supporting
atomicity. If the batch mutation fails due to an exception in the
storage backend, the entire transaction is failed.

If the primary persistence into the storage backend succeeds but
secondary persistence into the indexing backends or the logging system
fail, the transaction is still considered to be successful because the
storage backend is the authoritative source of the graph.

However, this can create inconsistencies with the indexes and logs. To
automatically repair such inconsistencies, JanusGraph can maintain a
transaction write-ahead log which is enabled through the configuration.
```properties
tx.log-tx = true
tx.max-commit-time = 10000
```

The max-commit-time property is used to determine when a transaction has
failed. If the persistence stage of the transaction takes longer than
this time, JanusGraph will attempt to recover it if necessary. Hence,
this time out should be configured as a generous upper bound on the
maximum duration of persistence. Note, that this does not include the
time spent before commit.

In addition, a separate process must be setup that reads the log to
identify partially failed transaction and repair any inconsistencies
caused. It is suggested to run the transaction repair process on a
separate machine connected to the cluster to isolate failures. Configure
a separately controlled process to run the following where the start
time (Java Instant that specifies the time since epoch) where the recovery
process should start reading from the write-ahead log.
```groovy
recovery = JanusGraphFactory.startTransactionRecovery(graph, startTime);
```

Once the recovery process is started, the process never ends and stops
only if:

1. it is manually stopped by calling `recovery.shutdown()`
2. the process encounters errors and fails due to exception
3. the graph gets closed

While the recovery process runs, `recovery.getStatistics()` call provides
information about the progress of recovery process by returning three numbers:

1. the first number shows how many secondary persistence transaction succeeded
2. the second number shows how many secondary persistence transaction failed
and attempted to be recovered
3. the third number shows how many failed secondary persistence transaction
could not be recovered

Depending on the used `startTime` value and configured `log.tx.read-interval`
configuration option, the recovery process might need to run for hours in
order to process all relevant entries from the write-ahead log.
`startTime` defines the point in time from which the write-ahead
log should be read. The log is read in every `log.tx.read-interval`
millisecond and an approximately 100 seconds long chunk is processed in
one iteration.

For example, if `startTime` is configured to look for log entries from
the last twenty hours (72 000 seconds) and `log.tx.read-interval` is set to
5 000 ms (5 seconds), it might take approximately at least one hour
(72 000 / 100 * 5 = 3 600 seconds = 1 hour) while all log entries are processed.
Note: in case there are many failed secondary persistence transactions, the recovery
process might take much longer as fixing those transactions takes time.

When a write-ahead log entry is found that should be repaired,
the following INFO level log message appears in the JanusGraph's
logging system where the transaction ID appears between the
squared brackets:

```
Attempting to repair partially failed transaction [...]
```

Even if the recovery process is stopped by calling `recovery.shutdown()`,
when it started again for the same graph `Provided read marker is not compatible
with existing read marker for previously registered readers` error is shown.
To avoid the error, the graph needs to be closed by `graph.close()` before the
process is started again.

Enabling the transaction write-ahead log causes an additional write
operation for mutating transactions which increases the latency. Also
note, that additional space is required to store the log. The
transaction write-ahead log has a configurable time-to-live of 2 days
which means that log entries expire after that time to keep the storage
overhead small. Refer to [Configuration Reference](../configs/configuration-reference.md) for a complete list of all
log related configuration options to fine tune logging behavior.

### Recurring Transaction Recovery

In case of daily data ingestion, transaction recovery needs to run recurring to ensure that
both primary and secondary persistence (e.g. indexing data by the mixed index backend)
of the data succeeds each day.

Since `JanusGraphFactory.startTransactionRecovery()` is not meant to be executed on
recurring way, JanusGraph provides a dedicated way to run transaction recovery multiple
times on the same graph:

```groovy
recovery = JanusGraphFactory.startRecurringTransactionRecovery(graph, startTime);
```

Similarly to the normal transaction recovery process, the recurring transaction recovery
process has the same `graph` and `startTime` parameters and provides the same `getStatistics()`
and `shutdown()` methods.

Once the process is stopped, `startRecurringTransactionRecovery()` can be used
to start the process again from the same or from another start time.

## JanusGraph Instance Failure

JanusGraph is robust against individual instance failure in that other
instances of the JanusGraph cluster are not impacted by such failure and
can continue processing transactions without loss of performance while
the failed instance is restarted.

However, some schema related operations - such as installing indexes -
require the coordination of all JanusGraph instances. For this reason,
JanusGraph maintains a record of all running instances. If an instance
fails, i.e. is not properly shut down, JanusGraph considers it to be
active and expects its participation in cluster-wide operations which
subsequently fail because this instances did not participate in or did
not acknowledge the operation.

In this case, the user must manually remove the failed instance record
from the cluster and then retry the operation. To remove the failed
instance, open a management transaction against any of the running
JanusGraph instances, inspect the list of running instances to identify
the failed one, and finally remove it.
```groovy
mgmt = graph.openManagement()
mgmt.getOpenInstances() //all open instances
==>7f0001016161-dunwich1(current)
==>7f0001016161-atlantis1
mgmt.forceCloseInstance('7f0001016161-atlantis1') //remove an instance
mgmt.commit()
```

The unique identifier of the current JanusGraph instance is marked with
the suffix `(current)` so that it can be easily identified. This
instance cannot be closed via the `forceCloseInstance` method and
instead should be closed via `g.close()`

It must be ensured that the manually removed instance is indeed no
longer active. Removing an active JanusGraph instance from a cluster can
cause data inconsistencies. Hence, use this method with great care in
particular when JanusGraph is operated in an environment where instances
are automatically restarted.
