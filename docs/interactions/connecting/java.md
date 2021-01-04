# Connecting from Java

While it is possible to embed JanusGraph as a library inside a Java
application and then directly connect to the backend, this section
assumes that the application connects to JanusGraph Server. For
information on how to embed JanusGraph, see the [JanusGraph Examples
projects](https://github.com/JanusGraph/janusgraph/tree/master/janusgraph-examples).

This section only covers how applications can connect to JanusGraph
Server using the [GraphBinary](http://tinkerpop.apache.org/docs/current/dev/io/#graphbinary) serialization. Refer to [Gremlin Query Language](../../getting-started/gremlin.md) for an introduction to Gremlin and
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

2.  Add dependencies on `janusgraph-driver` and `gremlin-driver` to the dependency manager:

    === "Maven"
        ```xml
        <dependency>
            <groupId>org.janusgraph</groupId>
            <artifactId>janusgraph-driver</artifactId>
            <version>{{ latest_version }}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.tinkerpop</groupId>
            <artifactId>gremlin-driver</artifactId>
            <version>{{ tinkerpop_version }}</version>
        </dependency>
        ```

    === "Gradle"
        ```groovy
        compile "org.janusgraph:janusgraph-driver:{{ latest_version }}"
        compile "org.apache.tinkerpop:gremlin-driver:{{ tinkerpop_version }}"
        ```

3.  Add two configuration files, `conf/remote-graph.properties` and
    `conf/remote-objects.yaml`:

    === "conf/remote-graph.properties"
        ```conf
        gremlin.remote.remoteConnectionClass=org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
        gremlin.remote.driver.clusterFile=conf/remote-objects.yaml
        gremlin.remote.driver.sourceName=g
        ```

    === "conf/remote-objects.yaml"
        ```yaml
        hosts: [localhost]
        port: 8182
        serializer: { 
            className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0,
            config: { ioRegistries: [org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] }}
        ```

4.  Create a `GraphTraversalSource` which is the basis for all Gremlin traversals:

    ```java
    import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

    GraphTraversalSource g = traversal().withRemote("conf/remote-graph.properties");
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

JanusGraph specific types and [predicates](../search-predicates.md) can be
used directly from a Java application through the dependency `janusgraph-driver`.


## Consideration for Accessing the Management API

The described connection uses [GraphBinary](http://tinkerpop.apache.org/docs/current/dev/io/#graphbinary) and the `janusgraph-driver` which doesn't allow accessing the internal JanusGraph components such as `ManagementSystem`. To access the `ManagementSystem`, you have to update the package and the serialization. 

* The maven package `janusgraph-driver` needed to be replaced with the maven package `janusgraph-core`. 
* Serialization class in the file `conf/remote-objects.yaml` have to be updated by replacing `className: org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1` with `className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0,`.
