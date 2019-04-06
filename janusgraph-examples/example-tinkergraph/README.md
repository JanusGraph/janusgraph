# TinkerGraph

## About TinkerGraph

[TinkerGraph](https://tinkerpop.apache.org/docs/3.2.6/reference/#tinkergraph-gremlin)
is the in-memory reference implementation of the Apache TinkerPop Graph API.
It can be useful with small graphs to prototype out a graph structure and
queries against it. It can also be useful as a comparison when debugging to
help determine whether a particular issue is in the graph traversal logic
or whether it is a JanusGraph-specific issue.

[`TinkerGraphApp.java`](src/main/java/org/janusgraph/example/TinkerGraphApp.java)
is an example Java program that connects to a TinkerGraph. You will notice
the major difference between this an the JanusGraph app is in how the graphs
define schema and indexes. The code to construct and query the graph is exactly
the same.

## TinkerGraph configuration

[`jgex-tinkergraph.properties`](conf/jgex-tinkergraph.properties) contains
the id manager settings for the graph.

Refer to the TinkerGraph [configuration reference](https://tinkerpop.apache.org/docs/3.2.6/reference/#_configuration_3)
for additional properties.

## Dependencies

The required Maven dependency for TinkerGraph:

```
        <dependency>
            <groupId>org.apache.tinkerpop</groupId>
            <artifactId>tinkergraph-gremlin</artifactId>
            <version>${tinkerpop.version}</version>
        </dependency>
```

## Run the example

This command can be run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-tinkergraph
```

## Drop the graph

TinkerGraph is not configured to not store anything into a persistent
location, so the graph will disappear when the application finishes.
