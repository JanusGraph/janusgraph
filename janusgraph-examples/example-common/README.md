# Common Example

## About the common example

[`GraphApp`](src/main/java/org/janusgraph/example/GraphApp.java) is an abstract
class that defines a basic structure for a graph application. It contains
methods for configuring a graph instance, defining a graph schema, creating
a graph structure, and querying a graph.

[`JanusGraphApp`](src/main/java/org/janusgraph/example/JanusGraphApp.java) is
a subclass of `GraphApp` using JanusGraph-specific methods to create the schema.

## In-Memory configuration

[`jgex-inmemory.properties`](conf/jgex-inmemory.properties) contains the
settings for the JanusGraph [in-memory storage backend](https://docs.janusgraph.org/storage-backend/inmemorybackend/).
This backend is primarily for testing purposes.

## Dependencies

The required Maven dependency for in-memory JanusGraph:

```
        <dependency>
            <groupId>org.janusgraph</groupId>
            <artifactId>janusgraph-core</artifactId>
            <version>${janusgraph.version}</version>
        </dependency>
```

## Run the example

This command can be run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-common
```

## Drop the graph

The in-memory JanusGraph does not store anything into a persistent location,
so the graph will disappear when the application finishes.
