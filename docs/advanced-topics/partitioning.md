# Graph Partitioning

When JanusGraph is deployed on a cluster of multiple storage backend instances, the graph is partitioned across those machines. Since JanusGraph stores the graph in an adjacency list representation the assignment of vertices to machines determines the partitioning. By default, JanusGraph uses a random partitioning strategy that randomly assigns vertices to machines. Random partitioning is very efficient, requires no configuration, and results in balanced partitions. Currently explicit partitioning is not supported.

```properties
cluster.max-partitions = 32
ids.placement = simple 
```

The configuration option `max-partitions` controls how many virtual partitions JanusGraph creates. This number should be roughly twice the number of storage backend instances. If the cluster of storage backend instances is expected to grow, estimate the size of the cluster in the foreseeable future and take this number as the baseline. Setting this number too large will unnecessarily fragment the cluster which can lead to poor performance. This number should be larger than the maximum expected number of nodes in the JanusGraph graph. It must be greater than 1 and a power of 2. 

There are two aspects to graph partitioning which can be individually
controlled: edge cuts and vertex cuts.

## Edge Cut

In assigning vertices to partitions one strives to optimize the
assignment such that frequently co-traversed vertices are hosted on the
same machine. Assume vertex A is assigned to machine 1 and vertex B is
assigned to machine 2. An edge between the vertices is called a **cut
edge** because its end points are hosted on separate machines.
Traversing this edge as part of a graph query requires communication
between the machines which slows down query processing. Hence, it is
desirable to reduce the edge cut for frequently traversed edges. That,
in turn, requires placing the adjacent vertices of frequently traversed
edges in the same partition.

Vertices are placed in a partition by way of the assigned vertex id. A
partition is essentially a sequential range of vertex ids. To place a
vertex in a particular partition, JanusGraph chooses an id from the
partition’s range of vertex ids. JanusGraph controls the
vertex-to-partition assignment through the configured placement
strategy. By default, vertices created in the same transaction are
assigned to the same partition. This strategy is easy to reason about
and works well in situations where frequently co-traversed vertices are
created in the same transaction - either by optimizing the loading
strategy to that effect or because vertices are naturally added to the
graph that way. However, the strategy is limited, leads to imbalanced
partitions when data is loaded in large transactions and not the optimal
strategy for many use cases. The user can provide a use case specific
vertex placement strategy by implementing the `IDPlacementStrategy`
interface and registering it in the configuration through the
`ids.placement` option.

When implementing `IDPlacementStrategy`, note that partitions are
identified by an integer id in the range from 0 to the number of
configured virtual partitions minus 1. For our example configuration,
there are partitions 0, 1, 2, 3, ..31. Partition ids are not the same as
vertex ids. Edge cuts are more meaningful when the JanusGraph servers
are on the same hosts as the storage backend. If you have to make a
network call to a different host on each hop of a traversal, the benefit
of edge cuts and custom placement strategies can be largely nullified.

## Vertex Cut

While edge cut optimization aims to reduce the cross communication and
thereby improve query execution, vertex cuts address the hotspot issue
caused by vertices with a large number of incident edges. While
[vertex-centric indexes](../schema/index-management/index-performance.md#vertex-centric-indexes) effectively address query
performance for large degree vertices, vertex cuts are needed to address
the hot spot issue on very large graphs.

Cutting a vertex means storing a subset of that vertex’s adjacency list
on each partition in the graph. In other words, the vertex and its
adjacency list is partitioned thereby effectively distributing the load
on that single vertex across all of the instances in the cluster and
removing the hot spot.

JanusGraph cuts vertices by label. A vertex label can be defined as
*partitioned* which means that all vertices of that label will be
partitioned across the cluster in the manner described above.

```java
mgmt = graph.openManagement()
mgmt.makeVertexLabel('user').make()
mgmt.makeVertexLabel('product').partition().make()
mgmt.commit()
```

In the example above, `product` is defined as a partitioned vertex label
whereas `user` is a normal label. This configuration is beneficial for
situations where there are thousands of products but millions of users
and one records transactions between users and products. In that case,
the product vertices will have a very high degree and the popular
products turns into hot spots if they are not partitioned.

## Graph Partitioning FAQ

### Random vs. Explicit Partitioning

When the graph is small or accommodated by a few storage instances, it
is best to use random partitioning for its simplicity. As a rule of
thumb, one should strongly consider enabling explicit graph partitioning
and configure a suitable partitioning heuristic when the graph grows
into the 10s of billions of edges.
