# Connecting from Java


While it is possible to embed JanusGraph as a library inside a Java
application and then directly connect to the backend, this section
assumes that the application connects to JanusGraph Server. For
information on how to embed JanusGraph, see the [JanusGraph Examples
projects](https://github.com/JanusGraph/janusgraph/tree/master/janusgraph-examples).

This section only covers how applications can connect to JanusGraph
Server. Refer to [Gremlin Query Language](../basics/gremlin.md) for an introduction to Gremlin and
pointers to further resources.

## Getting Started with JanusGraph and Gremlin-Java

To get started with JanusGraph in Java:

1.  Create an application with Maven:
```bash
mvn archetype:generate -DgroupId=com.mycompany.project
    -DartifactId=gremlin-example
    -DarchetypeArtifactId=maven-archetype-quickstart
    -DinteractiveMode=false
```
2.  Add dependencies on `janusgraph-core` and `gremlin-driver` to the dependency manager:

```xml tab='Maven'
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-core</artifactId>
    <version>{{ latest_version }}</version>
</dependency>
<dependency>
    <groupId>org.apache.tinkerpop</groupId>
    <artifactId>gremlin-driver</artifactId>
    <version>{{ tinkerpop_version }}</version>
</dependency>
```

```groovy tab='Gradle'
compile "org.janusgraph:janusgraph-core:{{ latest_version }}"
compile "org.apache.tinkerpop:gremlin-driver:{{ tinkerpop_version }}"
```

3.  Add two configuration files, `conf/remote-graph.properties` and
    `conf/remote-objects.yaml`:

```conf tab='conf/remote-graph.properties'
gremlin.remote.remoteConnectionClass=org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
gremlin.remote.driver.clusterFile=conf/remote-objects.yaml
gremlin.remote.driver.sourceName=g
```

```yaml tab='conf/remote-objects.yaml'
hosts: [localhost]
port: 8182
serializer: { 
    className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0,
    config: { ioRegistries: [org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] }}
```

4.  Create a `GraphTraversalSource` which is the basis for all Gremlin traversals:
```java
    Graph graph = EmptyGraph.instance();
    GraphTraversalSource g = graph.traversal().withRemote("conf/remote-graph.properties");
    // Reuse 'g' across the application
    // and close it on shut-down to close open connections with g.close()
```
5.  Execute a simple traversal:
```java
Object herculesAge = g.V().has("name", "hercules").values("age").next();
System.out.println("Hercules is " + herculesAge + " years old.");
```
`next()` is a terminal step that submits the traversal to the Gremlin Server and returns a single result.

## JanusGraph Specific Types and Predicates

JanusGraph specific types and [predicates](../index-backend/search-predicates.md) can be
used directly from a Java application through the dependency
`janusgraph-core`.
