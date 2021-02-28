# Dynamic Graphs

JanusGraph supports [dynamically creating graphs](configured-graph-factory.md#configuredgraphfactory). This is
deviation from the way in which standard Gremlin Server implementations
allow one to access a graph. Traditionally, users create bindings to
graphs at server-start, by configuring the gremlin-server.yaml file
accordingly. For example, if the `graphs` section of your yaml file
looks like this:
```yaml
graphs {
  graph1: conf/graph1.properties,
  graph2: conf/graph2.properties
}
```

then you will access your graphs on the Gremlin Server using the fact
that the String `graph1` will be bound to the graph opened on the server
as per its supplied properties file, and the same holds true for
`graph2`.

However, if we use the `ConfiguredGraphFactory` to dynamically create
graphs, then those graphs are managed by the
[JanusGraphManager](configured-graph-factory.md#janusgraphmanager) and
the graph configurations are managed by the
[ConfigurationManagementGraph](configured-graph-factory.md#configurationmanagementgraph).
This is especially useful because it 1. allows you to define graph
configurations post-server-start and 2. allows the graph configurations
to be managed in a persisted and distributed nature across your
JanusGraph cluster.

To properly use the `ConfiguredGraphFactory`, you must configure every
Gremlin Server in your cluster to use the `JanusGraphManager` and the
`ConfigurationManagementGraph`. This procedure is explained in detail
[here](configured-graph-factory.md#configuring-janusgraph-server-for-configuredgraphfactory).

## Graph Reference Consistency

If you configure all your JanusGraph servers to use the
[ConfiguredGraphFactory](configured-graph-factory.md#configuring-janusgraph-server-for-configuredgraphfactory),
JanusGraph will ensure all graph representations are-up-to-date across
all JanusGraph nodes in your cluster.

For example, if you update or delete the configuration to a graph on one
JanusGraph node, then we must evict that graph from the cache on *every
JanusGraph node in the cluster*. Otherwise, we may have inconsistent
graph representations across your cluster. JanusGraph automatically
handles this eviction using a messaging log queue through the backend
system that the graph in question is configured to use.

If one of your servers is configured incorrectly, then it may not be
able to successfully remove the graph from the cache.

!!! important
    Any updates to your
    [TemplateConfiguration](configured-graph-factory.md#template-configuration)
    will not result in the updating of graphs/graph configurations
    previously created using said template configuration. If you want to
    update the individual graph configurations, you must do so using the
    [available update
    APIs](configured-graph-factory.md#updating-configurations). These
    update APIs will *then* result in the graph cache eviction across all
    JanusGraph nodes in your cluster.

## Dynamic Graph and Traversal Bindings

JanusGraph has the ability to bind dynamically created graphs and their
traversal references to `<graph.graphname>` and
`<graph.graphname>_traversal`, respectively, across all JanusGraph nodes
in your cluster, with a maximum of a 20s lag for the binding to take
effect on any node in the cluster. Read more about this
[here](configured-graph-factory.md#graph-and-traversal-bindings).

JanusGraph accomplishes this by having each node in your cluster poll
the `ConfigurationManagementGraph` for all graphs for which you have
created configurations. The `JanusGraphManager` will then open said
graph with its persisted configuration, store it in its graph cache, and
bind the `<graph.graphname>` to the graph reference on the
`GremlinExecutor` as well as bind `<graph.graphname>_traversal` to the
graph’s traversal reference on the `GremlinExecutor`.

This allows you to access a dynamically created graph and its traversal
reference by their string bindings, on every node in your JanusGraph
cluster. This is particularly important to be able to work with Gremlin
Server clients and use [TinkerPops’s withRemote functionality](#using-tinkerpops-withremote-functionality).

!!! note
    To set up your cluster to bind dynamically created graphs and their
    traversal references, you must configure each node to use 
    the [ConfiguredGraphFactory](configured-graph-factory.md#configuring-janusgraph-server-for-configuredgraphfactory).

### Using TinkerPop’s withRemote Functionality

Since traversal references are bound on the JanusGraph servers, we can
make use of [TinkerPop’s withRemote
functionality](https://tinkerpop.apache.org/docs/{{ tinkerpop_version }}/reference/#connecting-via-remotegraph).
This will allow one to run gremlin queries locally, against a remote
graph reference. Traditionally, one runs queries against remote Gremlin
Servers by sending String script representations, which are processed on
the remote server and the response serialized and sent back. However,
TinkerPop also allows for the use of `remoteGraph`, which could be
useful if you are building a TinkerPop compliant graph infrastructure
that is easily transferable to multiple implementations.

To use this functionality in JanusGraph, we must first ensure we have
created a graph on the remote JanusGraph cluster:

``` ConfiguredGraphFactory.create("graph1"); ```

Next, we must wait 20 seconds to ensure the traversal reference is bound
on every JanusGraph node in the remote cluster.

Finally, we can locally make use of the `withRemote` method to access a
local reference to a remote graph:
```groovy
gremlin> cluster = Cluster.open('conf/remote-objects.yaml')
==>localhost/127.0.0.1:8182
gremlin> g = traversal().withRemote(DriverRemoteConnection.using(cluster, "graph1_traversal"))
==>graphtraversalsource[emptygraph[empty], standard]
```

For completion, the above `conf/remote-objects.yaml` should tell the
`Cluster` API how to access the remote JanusGraph servers; for example,
it may look like:
```yaml
hosts: [remoteaddress1.com, remoteaddress2.com]
port: 8182
username: admin
password: password
connectionPool: { enableSsl: true }
serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0, config: { ioRegistries: [org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] }}
```
