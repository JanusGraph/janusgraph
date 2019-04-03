# JanusGraph Bus

The **JanusGraph Bus** describes a collection of configurable logs to which JanusGraph writes changes to the graph and its management. The JanusGraph Bus is used for internal (i.e. between multiple JanusGraph instances) and external (i.e. integration with other systems) communication.

In particular, JanusGraph maintains three separate logs:

## Trigger Log
The purpose of the trigger log is to capture the mutations of a transaction so that the resulting changes to the graph can trigger events in other system. Such events may be propagating the change to other data stores, view maintenance, or aggregate computation.

The trigger log consists of multiple sub-logs as configured by the user. When opening a transaction, the identifier for the trigger sub-log can be specified:
```java
tx = g.buildTransaction().logIdentifier("purchase").start();
```

In this case, the identifier is "purchase" which means that the mutations of this transaction will be written to a log with the name "trigger_purchase". This gives the user control over where transactional mutations are logged. If no trigger log is specified, no trigger log entry will be created.

## Transaction Log

The transaction log is maintained by JanusGraph and contains two entries for each transaction if enabled:
1. Pre-Commit: Before the changes are persisted to the storage and indexing backends, the changes are compiled and written to the log.
2. Post-Commit: The success status of the transaction is written to the log.

In this way, the transaction log functions as a Write-Ahead-Log (WAL). This log is not meant for consumption by the user or external systems - use trigger logs for that. It is used internally to store partial transaction persistence against eventually consistent backends.

The transaction log can be enabled via the root-level configuration option "log-tx".

## Management Log

The management log is maintained by JanusGraph internally to communicate and persist all changes to global configuration options or the graph schema.
