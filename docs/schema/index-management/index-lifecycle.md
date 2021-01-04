# Index Lifecycle

JanusGraph uses only indexes which have status `ENABLED`. 
When the index is created it will not be used by JanusGraph until it is enabled. 
After the index is build you should wait until it is registered (i.e. available) by JanusGraph:
```java
//Wait for the index to become available (i.e. wait for status REGISTERED)
ManagementSystem.awaitGraphIndexStatus(graph, "myIndex").call();
```

After the index is registered we should either enable the index (if we are sure that the current data should not be indexed by the newly created index) or we should reindex current data so that it would be available in the newly created index.

Reindex the existing data and automatically enable the index example:
```java
mgmt = graph.openManagement();
mgmt.updateIndex(mgmt.getGraphIndex("myIndex"), SchemaAction.REINDEX).get();
mgmt.commit();
```

Enable the index without reindexing existing data example:
```java
mgmt = graph.openManagement();
mgmt.updateIndex(mgmt.getGraphIndex("myAnotherIndex"), SchemaAction.ENABLE_INDEX).get();
mgmt.commit();
```

## Index states and transitions

![States and transitions](index-lifecycle.png)

## States (SchemaStatus)
An index can be in one of the following states:

-   **INSTALLED** The index is installed in the system but not yet registered with all instances in the cluster

-   **REGISTERED** The index is registered with all instances in the cluster but not (yet) enabled

-   **ENABLED** The index is enabled and in use

-   **DISABLED** The index is disabled and no longer in use

## Actions (SchemaAction)
The following actions can be performed on an index to change its state via `mgmt.updateIndex()`:

-   **REGISTER_INDEX** Registers the index with all instances in the graph cluster. After an index is installed, it must be registered with all graph instances

-   **REINDEX** Re-builds the index from the graph

-   **ENABLE_INDEX** Enables the index so that it can be used by the query processing engine. An index must be registered before it can be enabled.

-   **DISABLE_INDEX** Disables the index in the graph so that it is no longer used.

-   **REMOVE_INDEX** Removes the index from the graph (optional operation). Only on composite index.
