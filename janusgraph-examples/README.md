# JanusGraph Examples

The JanusGraph examples show the basics of how to configure and construct
a graph application. It uses [Apache Maven](https://maven.apache.org) to
manage the numerous dependencies required to build the application. The common
application will:

* Open and initialize the graph
* Define the schema
* Build the graph
* Run traversal queries to get data from the graph
* Make updates to the graph
* Close the graph

By using different graph configurations, the same example code can run against
the various supported storage and indexing backends.

## Prerequisites

* Java 8 Developer Kit, update 40 or higher
* Apache Maven, version 3.3 or higher

## Building the Examples

```
mvn clean install
```

## Running the Examples

Refer to the directions in each sub-directory.

* [Common](example-common/README.md)
* [BerkeleyJE](example-berkeleyje/README.md)
* [Cassandra](example-cassandra/README.md)
* [CQL](example-cql/README.md)
* [HBase](example-hbase/README.md)
* [RemoteGraph](example-remotegraph/README.md)
* [TinkerGraph](example-tinkergraph/README.md)
