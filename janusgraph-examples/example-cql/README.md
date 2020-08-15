# Cassandra CQL Storage, Elasticsearch Index

## About Cassandra and Elasticsearch

[Apache Cassandra](https://cassandra.apache.org/) is a distributed database
designed for scalability and high availability. Cassandra supports two
protocols for communications, Thrift (legacy RPC protocol) and CQL (native
protocol).

[Elasticsearch](https://www.elastic.co/elasticsearch/) is a scalable,
distributed search engine.

> Check the JanusGraph [version compatibility](https://docs.janusgraph.org/changelog/#version-compatibility)
to ensure you select versions of Cassandra and Elasticsearch compatible with
this JanusGraph release.

## JanusGraph configuration

* [`jgex-cql.properties`](conf/jgex-cql.properties) contains the Cassandra
and Elasticsearch server locations. By providing different values for
`storage.cql.keyspace` and `index.jgex.index-name`, you can store multiple
graphs on the same Cassandra and Elasticsearch servers. Refer to the JanusGraph
[configuration reference](https://docs.janusgraph.org/basics/configuration-reference/)
for additional properties.

* [`logback.xml`](conf/logback.xml) configures logging with [Logback](https://logback.qos.ch/),
which is the logger used by Cassandra. The example configuration logs to the
console and adjusts the logging level for some noisier packages. Refer to
the Logback [manual](https://logback.qos.ch/manual/index.html) for additional
details.

### Cassandra configuration

The JanusGraph properties file assumes that Cassandra is installed on localhost
using its default configuration. Please refer to the Cassandra documentation
for installation instructions.

### Elasticsearch configuration

The JanusGraph properties file assumes that Elasticsearch is installed on
localhost using its default configuration. Please refer to the Elasticsearch
documentation for installation instructions.

### JanusGraph pre-packaged distribution

Rather than installing Cassandra and Elasticsearch separately, the JanusGraph
[pre-packaged distribution](https://docs.janusgraph.org/basics/server/#using-the-pre-packaged-distribution)
is provided for convenience. The distribution starts a local Cassandra,
Elasticsearch, and Gremlin Server.

## Dependencies

The required Maven dependency for Cql:

```
        <dependency>
            <groupId>org.janusgraph</groupId>
            <artifactId>janusgraph-cql</artifactId>
            <version>${janusgraph.version}</version>
            <scope>runtime</scope>
        </dependency>
```

The required Maven dependency for Elasticsearch:

```
        <dependency>
            <groupId>org.janusgraph</groupId>
            <artifactId>janusgraph-es</artifactId>
            <version>${janusgraph.version}</version>
            <scope>runtime</scope>
        </dependency>
```

## Run the example

This command can be run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-cql
```

## Drop the graph

After running an example, you may want to drop the graph from storage. Make
sure to stop the application before dropping the graph. This command can be
run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-cql -Dcmd=drop
```
