InMemory Storage Backend
========================

JanusGraph ships with an in-memory storage backend which can be used
through the following configuration:

```conf
storage.backend=inmemory
```

Alternatively, an in-memory JanusGraph graph can be opened directly in
the Gremlin Console:

```java
graph = JanusGraphFactory.build().set('storage.backend', 'inmemory').open()
```

There are no additional configuration options for the in-memory storage
backend. As the name suggests, this backend holds all data in memory.
Shutting down the graph or terminating the process that hosts the
JanusGraph graph will irrevocably delete all data from the graph. This
backend is local to a particular JanusGraph graph instance and cannot be
shared across multiple JanusGraph graphs.

Ideal Use Case
--------------

The in-memory storage backend was primarily developed to simplify
testing (for those tests that do not require persistence) and graph
exploration. The in-memory storage backend is NOT meant for production
use, large graphs, or high performance use cases. The in-memory storage
backend is not performance or memory optimized. All data is stored in
the heap space allocated to the Java virtual machine.
