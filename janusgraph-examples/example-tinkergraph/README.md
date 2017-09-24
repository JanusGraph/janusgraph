# TinkerGraph

## About TinkerGraph

[TinkerGraph](http://tinkerpop.apache.org/docs/3.2.6/reference/#tinkergraph-gremlin)
is the in-memory reference implementation of the Apache TinkerPop Graph API.
It can be useful with small graphs to prototype out a graph structure and
queries against it. It can also be useful as a comparison when debugging to
help determine whether a particular issue is in the graph traversal logic
or whether it is a JanusGraph-specific issue.

## TinkerGraph configuration

[`jgex-tinkergraph.properties`](conf/jgex-tinkergraph.properties) contains
the id manager settings for the graph.

Refer to the TinkerGraph [configuration reference](http://tinkerpop.apache.org/docs/3.2.6/reference/#_configuration_3)
for additional properties.

## Running the example

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-tinkergraph

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.TinkerGraphApp" -Dexec.args="conf/jgex-tinkergraph.properties"
```
