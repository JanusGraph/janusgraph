# InMemory Storage Backend

JanusGraph ships with an in-memory storage backend which can be used
through the following configuration:

```properties
storage.backend=inmemory
```

Alternatively, an in-memory JanusGraph graph can be opened directly in
the Gremlin Console:

```java
graph = JanusGraphFactory.build().set('storage.backend', 'inmemory').open()
```

There are no additional configuration options for the in-memory storage
backend. As the name suggests, this backend holds all data in memory, specifically
 in the heap space allocated to the java virtual machine running Janusgraph instance.

Shutting down the graph or terminating the process that hosts the
JanusGraph graph will irrevocably delete all data from the graph. This
backend is local to a particular JanusGraph graph instance and cannot be
shared across multiple JanusGraph graphs.

## Ideal Use Case
### Rapid testing

The in-memory storage backend was initially developed to simplify
testing (for those tests that do not require persistence) and graph
exploration. Automated testing and ad-hoc prototyping remains its main purpose within Janusgraph project.

### Production

The initial test-only implementation was further evolved to use a more compact
 in-memory representation, which made it suitable for production use in certain scenarios, such as 
 Janusgraph engine being embedded into an application process, or spawned dynamically on demand, where:

- Ease of setup/configuration/maintenance/start up is important (just run Janusgraph from distribution jar, 
no management of clusters for backend DBs etc)  
- Loss of data due to _unexpected_ death of host process is acceptable (the backend provides a 
simple mechanism for making fast snapshots to handle _expected_ restarts, and the data can always 
be exported on Gremlin/Tinkerpop level to say GraphSON)  
- Size of the graph data makes it possible to host it in a single JVM process (i.e. a few tens of Gigabytes max, 
unless you use a specialized JVM and hardware)  
- Higher performance is required, but no expertise/resources available to tune more complex backends. 
Due to its memory-only nature, in-memory backend typically performs faster than disk-based ones, 
in queries using simple indices and in graph modifications. However it is not specifically optimized for performance, 
and does not support advanced indexing functionality. 

### Limitations

- Obviously the scalability is limited to the heap size of a single JVM, 
and no transparent resilience to failures is offered  
- The backend offers store-level locking only, whereas a Janusgraph transaction typically changes multiple stores 
(e.g. vertex store and index store). This means that in scenarios where data is modified in parallel transactions, 
care should be taken on application level to avoid conflicting updates. At a high level, this means that you can 
modify unrelated parts of the graph in parallel, but you should avoid changing the same vertex or 
its edges in parallel transactions.  
- The backend does not guarantee a clean rollback once commit has started. Chances of a failure in the middle of commit 
to an in-memory data structure are low, however this can happen - e.g. when a large heap nears saturation and the 
GC pause exceeds configured backend timeout.  
- The data layout used by the backend can theoretically be susceptible to fragmentation in certain scenarios
 (with a lot of add/delete operations), thus reducing the amount of useful data that can be stored in a heap
  of specified size. The backend provides simple mechanisms to report fragmentation and defragment the storage if required.
  However scenarios where the level of fragmentation is high enough to be an issue are expected to be rare,
   and usually defragmentation is not required at all.

### Alternatives

Generally, except for the above class of use cases, there doesn't seem to be much point in specifically 100% in-memory-only backend.
Many of the other supported backends, based on mature key-value databases, 
can be tuned to provide high performance and resiliency, plus advanced indexing capabilities etc 
(provided that you have resources and expertise to host and tune them, or use them as a service).  
However there are a few in-memory alternatives, such as (not exhaustive list, and in no particular order):

- BerkeleyJE backend configured with je.log.memOnly set to true 
(care should be taken to avoid using it for scenarios with continuous write operations, even if the effective size of 
the data remains the same - continuous write operations can drive uncontrolled log growth, leading to OOM). 
The backend has dump/load capability, and supports compression.  
- [Aerospike-based backend](https://github.com/Playtika/aerospike-janusgraph-storage-backend) (not part of Janugraph but a separate project)  
 