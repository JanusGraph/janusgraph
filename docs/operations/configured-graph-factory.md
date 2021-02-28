# ConfiguredGraphFactory

The JanusGraph Server can be configured to use the
`ConfiguredGraphFactory`. The `ConfiguredGraphFactory` is an access
point to your graphs, similar to the `JanusGraphFactory`. These graph
factories provide methods for dynamically managing the graphs hosted on
the server.

## Overview

`JanusGraphFactory` is a class that provides an access point to your
graphs by providing a Configuration object each time you access the
graph.

`ConfiguredGraphFactory` provides an access point to your graphs for
which you have previously created configurations using the
`ConfigurationManagementGraph`. It also offers an access point to manage
graph configurations.

`ConfigurationManagementGraph` allows you to manage graph
configurations.

`JanusGraphManager` is an internal server component that tracks graph
references, provided your graphs are configured to use it.

## ConfiguredGraphFactory versus JanusGraphFactory

However, there is an important distinction between these two graph
factories:

1.  The `ConfiguredGraphFactory` can only be used if you have configured
    your server to use the `ConfigurationManagementGraph` APIs at server
    start.

The benefits of using the `ConfiguredGraphFactory` are that:

1.  You only need to supply a `String` to access your graphs, as opposed
    to the `JanusGraphFactory`-- which requires you to specify
    information about the backend you wish to use when accessing a
    graph-- every time you open a graph.

2.  If your ConfigurationManagementGraph is configured with a
    distributed storage backend then your graph configurations are
    available to all JanusGraph nodes in your cluster.

## How Does the ConfiguredGraphFactory Work?

The `ConfiguredGraphFactory` provides an access point to graphs under
two scenarios:

1.  You have already created a configuration for your specific graph
    object using the `ConfigurationManagementGraph#createConfiguration`.
    In this scenario, your graph is opened using the previously created
    configuration for this graph.

2.  You have already created a template configuration using the
    `ConfigurationManagementGraph#createTemplateConfiguration`. In this
    scenario, we create a configuration for the graph you are creating
    by copying over all attributes stored in your template configuration
    and appending the relevant graphName attribute, and we then open the
    graph according to that specific configuration.

## Accessing the Graphs

You can either use `ConfiguredGraphFactory.create("graphName")` or
`ConfiguredGraphFactory.open("graphName")`. Learn more about the
difference between these two options by reading the section below about
the `ConfigurationManagementGraph`.

You can also access your graphs by using the bindings. Read more about this in the [Graph and Traversal Bindings](#graph-and-traversal-bindings) section.

Listing the Graphs

`ConfiguredGraphFactory.getGraphNames()` will return a set of graph
names for which you have created configurations using the
`ConfigurationManagementGraph` APIs.

`JanusGraphFactory.getGraphNames()` on the other hand returns a set of
graph names for which you have instantiated *and* the references are
stored inside the `JanusGraphManager`.

## Dropping a Graph

`ConfiguredGraphFactory.drop("graphName")` will drop the graph database,
deleting all data in storage and indexing backends. The graph can be
open or closed (will be closed as part of the drop operation).
Furthermore, this will also remove any existing graph configuration in
the `ConfigurationManagementGraph`.

!!! important
    This is an irreversible operation that will delete all graph and index
    data.

!!! important
    To ensure all graph representations are consistent across all JanusGraph 
    nodes in your cluster, this removes the graph from the `JanusGraphManager` 
    graph cache on every node in the cluster, assuming each node has been 
    properly configured to use the `JanusGraphManager`. Learn more about this 
    feature and how to configure your server to use said feature [here](./dynamic-graphs.md#graph-reference-consistency).

## Configuring JanusGraph Server for ConfiguredGraphFactory

!!! note
    Starting with JanusGraph v0.6.0, every JanusGraph Server is started with 
    the JanusGraphManager, if `ConfigurationManagementGraph`is configured in 
    the `graphs` section and the default GraphManager is not overwritten.

To be able to use the `ConfiguredGraphFactory`, you must configure your
server to use the `ConfigurationManagementGraph` APIs. To do this, you
have to inject a graph variable named "ConfigurationManagementGraph" in
your server’s YAML’s `graphs` map. For example:

```yaml
graphManager: org.janusgraph.graphdb.management.JanusGraphManager
graphs: {
  ConfigurationManagementGraph: conf/JanusGraph-configurationmanagement.properties
}
```

In this example, our `ConfigurationManagementGraph` graph will be
configured using the properties stored inside
`conf/JanusGraph-configurationmanagement.properties`, which for example,
look like:

```properties
gremlin.graph=org.janusgraph.core.ConfiguredGraphFactory
storage.backend=cql
graph.graphname=ConfigurationManagementGraph
storage.hostname=127.0.0.1
```

Assuming the GremlinServer started successfully and the
`ConfigurationManagementGraph` was successfully instantiated, then all
the APIs available on the `ConfigurationManagementGraph` `Singleton`
will also act upon said graph. Furthermore, this is the graph that will
be used to access the configurations used to create/open graphs using
the `ConfiguredGraphFactory`.

!!! important
    The `pom.xml` included in the JanusGraph distribution lists this
    dependency as optional, but the `ConfiguredGraphFactory` makes use of
    the `JanusGraphManager`, which requires a declared dependency on the
    `org.apache.tinkerpop:gremlin-server`. So if you run into
    `NoClassDefFoundError` errors, then be sure to update according to
    this message.

## ConfigurationManagementGraph

The `ConfigurationManagementGraph` is a `Singleton` that allows you to
create/update/remove configurations that you can use to access your
graphs using the `ConfiguredGraphFactory`. See above on configuring your
server to enable use of these APIs.

!!! important
    The ConfiguredGraphFactory offers an access point to manage your graph
    configurations managed by the `ConfigurationManagementGraph`, so
    instead of acting upon the `Singleton` itself, you may act upon the
    corresponding `ConfiguredGraphFactory` static methods. For example,
    you may use `ConfiguredGraphFactory.removeTemplateConfiguration()`
    instead of
    `ConfiguredGraphFactory.getInstance().removeTemplateConfiguration()`.

### Graph Configurations

The `ConfigurationManagementGraph` singleton allows you to create
configurations used to open specific graphs, referenced by the
`graph.graphname` property. For example:

```groovy
map = new HashMap<String, Object>();
map.put("storage.backend", "cql");
map.put("storage.hostname", "127.0.0.1");
map.put("graph.graphname", "graph1");
ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
```

Then you could access this graph on any JanusGraph node using:
```groovy
ConfiguredGraphFactory.open("graph1");
```

### Template Configuration

The `ConfigurationManagementGraph` also allows you to create one
template configuration, which you can use to create many graphs using
the same configuration template. For example:
```groovy
map = new HashMap<String, Object>();
map.put("storage.backend", "cql");
map.put("storage.hostname", "127.0.0.1");
ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
```

After doing this, you can create graphs using the template
configuration:
```groovy
ConfiguredGraphFactory.create("graph2");
```

This method will first create a new configuration for "graph2" by
copying over all the properties associated with the template
configuration and storing it on a configuration for this specific graph.
This means that this graph can be accessed in, on any JanusGraph node,
in the future by doing:
```groovy
ConfiguredGraphFactory.open("graph2");
```

### Updating Configurations

All interactions with both the `JanusGraphFactory` and the
`ConfiguredGraphFactory` that interact with configurations that define
the property `graph.graphname` go through the `JanusGraphManager` which
keeps track of graph references created on the given JVM. Think of it as
a graph cache. For this reason:

!!! important
    Any updates to a graph configuration results in the eviction of the relevant graph from the graph cache on 
    every node in the JanusGraph cluster, assuming each node has been configured properly to use the `JanusGraphManager`. 
    Learn more about this feature and how to configure your server to use said feature [here](./dynamic-graphs.md#graph-reference-consistency).
    

Since graphs created using the template configuration first create a
configuration for that graph in question using a copy and create method,
this means that:

!!! important
    Any updates to a specific graph created using the template
    configuration are not guaranteed to take effect on the specific graph
    until:
    1. The relevant configuration is removed: `ConfiguredGraphFactory.removeConfiguration("graph2");`
    2. The graph is recreated using the template configuration: `ConfiguredGraphFactory.create("graph2");`

### Update Examples

1) We migrated our Cassandra data to a new server with a new IP address:

```java
map = new HashMap();
map.put("storage.backend", "cql");
map.put("storage.hostname", "127.0.0.1");
map.put("graph.graphname", "graph1");
ConfiguredGraphFactory.createConfiguration(new
MapConfiguration(map));

g1 = ConfiguredGraphFactory.open("graph1");

// Update configuration
map = new HashMap();
map.put("storage.hostname", "10.0.0.1");
ConfiguredGraphFactory.updateConfiguration("graph1",
map);

// We are now guaranteed to use the updated configuration
g1 = ConfiguredGraphFactory.open("graph1");
```

2) We added an Elasticsearch node to our setup:
```java
map = new HashMap();
map.put("storage.backend", "cql");
map.put("storage.hostname", "127.0.0.1");
map.put("graph.graphname", "graph1");
ConfiguredGraphFactory.createConfiguration(new
MapConfiguration(map));

g1 = ConfiguredGraphFactory.open("graph1");

// Update configuration
map = new HashMap();
map.put("index.search.backend", "elasticsearch");
map.put("index.search.hostname", "127.0.0.1");
map.put("index.search.elasticsearch.transport-scheme", "http");
ConfiguredGraphFactory.updateConfiguration("graph1",
map);

// We are now guaranteed to use the updated configuration
g1 = ConfiguredGraphFactory.open("graph1");
```

3) Update a graph configuration that was created using a template
configuration that has been updated:
```java
map = new HashMap();
map.put("storage.backend", "cql");
map.put("storage.hostname", "127.0.0.1");
ConfiguredGraphFactory.createTemplateConfiguration(new
MapConfiguration(map));

g1 = ConfiguredGraphFactory.create("graph1");

// Update template configuration
map = new HashMap();
map.put("index.search.backend", "elasticsearch");
map.put("index.search.hostname", "127.0.0.1");
map.put("index.search.elasticsearch.transport-scheme", "http");
ConfiguredGraphFactory.updateTemplateConfiguration(new
MapConfiguration(map));

// Remove Configuration
ConfiguredGraphFactory.removeConfiguration("graph1");

// Recreate
ConfiguredGraphFactory.create("graph1");
// Now this graph's configuration is guaranteed to be updated
```

## JanusGraphManager

The `JanusGraphManager` is a `Singleton` adhering to the TinkerPop
graphManager specifications.

In particular, the `JanusGraphManager` provides:

1.  a coordinated mechanism by which to instantiate graph references on
    a given JanusGraph node

2.  a graph reference tracker (or cache)

Any graph you create using the `graph.graphname` property will go
through the `JanusGraphManager` and thus be instantiated in a
coordinated fashion. The graph reference will also be placed in the
graph cache on the JVM in question.

Thus, any graph you open using the `graph.graphname` property that has
already been instantiated on the JVM in question will be retrieved from
the graph cache.

This is why updates to your configurations require a few steps to
guarantee correctness.

### How To Use The JanusGraphManager

This is a new configuration option you can use when defining a property
in your configuration that defines how to access a graph. All
configurations that include this property will result in the graph
instantiation happening through the `JanusGraphManager` (process
explained above).

For backwards compatibility, any graphs that do not supply this
parameter but supplied at server start in your graphs object in your
.yaml file, these graphs will be bound through the JanusGraphManager
denoted by their `key` supplied for that graph. For example, if your
.yaml graphs object looks like:

```yaml
graphManager: org.janusgraph.graphdb.management.JanusGraphManager
graphs {
  graph1: conf/graph1.properties,
  graph2: conf/graph2.properties
}
```

but `conf/graph1.properties` and `conf/graph2.properties` do not include
the property `graph.graphname`, then these graphs will be stored in the
JanusGraphManager and thus bound in your gremlin script executions as
`graph1` and `graph2`, respectively.

### Important

For convenience, if your configuration used to open a graph specifies
`graph.graphname`, but does not specify the backend’s storage directory,
tablename, or keyspacename, then the relevant parameter will
automatically be set to the value of `graph.graphname`. However, if you
supply one of those parameters, that value will always take precedence.
And if you supply neither, they default to the configuration option’s
default value.

One special case is `storage.root` configuration option. This is a new
configuration option used to specify the base of the directory that will
be used for any backend requiring local storage directory access. If you
supply this parameter, you must also supply the `graph.graphname`
property, and the absolute storage directory will be equal to the value
of the `graph.graphname` property appended to the value of the
`storage.root` property.

Below are some example use cases:

1) Create a template configuration for my Cassandra backend such that
each graph created using this configuration gets a unique keyspace
equivalent to the `String` &lt;graphName&gt; provided to the factory:
```groovy
map = new HashMap();
map.put("storage.backend", "cql");
map.put("storage.hostname", "127.0.0.1");
ConfiguredGraphFactory.createTemplateConfiguration(new
MapConfiguration(map));

g1 = ConfiguredGraphFactory.create("graph1"); //keyspace === graph1
g2 = ConfiguredGraphFactory.create("graph2"); //keyspace === graph2
g3 = ConfiguredGraphFactory.create("graph3"); //keyspace === graph3
```

2) Create a template configuration for my BerkeleyJE backend such that
each graph created using this configuration gets a unique storage
directory equivalent to the
"&lt;storage.root&gt;/&lt;graph.graphname&gt;":
```groovy
map = new HashMap();
map.put("storage.backend", "berkeleyje");
map.put("storage.root", "/data/graphs");
ConfiguredGraphFactory.createTemplateConfiguration(new
MapConfiguration(map));

g1 = ConfiguredGraphFactory.create("graph1"); //storage directory === /data/graphs/graph1
g2 = ConfiguredGraphFactory.create("graph2"); //storage directory === /data/graphs/graph2
g3 = ConfiguredGraphFactory.create("graph3"); //storage directory === /data/graphs/graph3
```


## Graph and Traversal Bindings

Graphs created using the ConfiguredGraphFactory are bound to the executor context on the Gremlin Server by the "graph.graphname" property, and the graph's traversal reference is bound to the context by "`<graphname>_traversal`". This means, on subsequent connections to the server after the first time you create/open a graph, you can access the graph and traversal references by the "`<graphname>`" and "`<graphname>_traversal`" properties.

Learn more about this feature and how to configure your server to use said feature [here](./dynamic-graphs.md#dynamic-graph-and-traversal-bindings).

!!! IMPORTANT
    If you are connected to a remote Gremlin Server using the Gremlin Console and a **sessioned** connection, then you will have to reconnect to the server to bind the variables. This is also true for any sessioned WebSocket connection.

!!! IMPORTANT
    The JanusGraphManager rebinds every graph stored on the ConfigurationManagementGraph (or those for which you have created configurations) every 20 seconds. This means your graph and traversal bindings for graphs created using the ConfiguredGraphFactory will be available on all JanusGraph nodes with a maximum of a 20 second lag. It also means that a binding will still be available on a node after a server restart.


### Binding Example

```groovy
gremlin> :remote connect tinkerpop.server conf/remote.yaml
==>Configured localhost/127.0.0.1:8182
gremlin> :remote console
==>All scripts will now be sent to Gremlin Server - [localhost/127.0.0.1:8182] - type ':remote console' to return to local mode
gremlin> ConfiguredGraphFactory.open("graph1")
==>standardjanusgraph[cql:[127.0.0.1]]
gremlin> graph1
==>standardjanusgraph[cql:[127.0.0.1]]
gremlin> graph1_traversal
==>graphtraversalsource[standardjanusgraph[cql:[127.0.0.1]], standard]
```

## Examples

It is recommended to use a sessioned connection when creating a
Configured Graph Factory template. If a sessioned connection is not used
the Configured Graph Factory Template creation must be sent to the
server as a single line using semi-colons. See details on sessions can
be found in [Connecting to Gremlin Server](../interactions/connecting/index.md).

```groovy
gremlin> :remote connect tinkerpop.server conf/remote.yaml session
==>Configured localhost/127.0.0.1:8182

gremlin> :remote console
==>All scripts will now be sent to Gremlin Server - [localhost:8182]-[5206cdde-b231-41fa-9e6c-69feac0fe2b2] - type ':remote console' to return to local mode

gremlin> ConfiguredGraphFactory.open("graph");
Please create configuration for this graph using the
ConfigurationManagementGraph API.

gremlin> ConfiguredGraphFactory.create("graph");
Please create a template Configuration using the
ConfigurationManagementGraph API.

gremlin> map = new HashMap();
gremlin> map.put("storage.backend", "cql");
gremlin> map.put("storage.hostname", "127.0.0.1");
gremlin> map.put("GraphName", "graph1");
gremlin> ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
Please include in your configuration the property "graph.graphname".

gremlin> map = new HashMap();
gremlin> map.put("storage.backend", "cql");
gremlin> map.put("storage.hostname", "127.0.0.1");
gremlin> map.put("graph.graphname", "graph1");
gremlin> ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
==>null

gremlin> ConfiguredGraphFactory.open("graph1").vertices();

gremlin> map = new HashMap(); map.put("storage.backend",
"cql"); map.put("storage.hostname", "127.0.0.1");
gremlin> map.put("graph.graphname", "graph1");
gremlin> ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
Your template configuration may not contain the property
"graph.graphname".

gremlin> map = new HashMap();
gremlin> map.put("storage.backend",
"cql"); map.put("storage.hostname", "127.0.0.1");
gremlin> ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
==>null

// Each graph is now acting in unique keyspaces equivalent to the
graphnames.
gremlin> g1 = ConfiguredGraphFactory.open("graph1");
gremlin> g2 = ConfiguredGraphFactory.create("graph2");
gremlin> g3 = ConfiguredGraphFactory.create("graph3");
gremlin> g2.addVertex();
gremlin> l = [];
gremlin> l << g1.vertices().size();
==>0
gremlin> l << g2.vertices().size();
==>1
gremlin> l << g3.vertices().size();
==>0

// After a graph is created, you must access it using .open()
gremlin> g2 = ConfiguredGraphFactory.create("graph2"); g2.vertices().size();
Configuration for graph "graph2" already exists.

gremlin> g2 = ConfiguredGraphFactory.open("graph2"); g2.vertices().size();
==>1
```