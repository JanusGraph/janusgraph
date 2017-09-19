# Common Example

## About the common example

`GraphApp` is an abstract class that defines a basic structure for a graph
application. It contains methods for configuring a graph instance, defining
a graph schema, creating a graph structure, and querying a graph.

`JanusGraphApp` is a subclass of `GraphApp` using JanusGraph-specific methods
to create the schema.

## In-Memory configuration

[`jgex-inmemory.properties`](conf/jgex-inmemory.properties) contains the
settings for the JanusGraph [in-memory storage backend](http://docs.janusgraph.org/latest/inmemorystorage.html).
This backend is primarily for testing purposes.

## Running the example

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-common

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dexec.args="conf/jgex-inmemory.properties"
```
