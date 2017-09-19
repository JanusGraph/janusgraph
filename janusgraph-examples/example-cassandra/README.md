# Cassandra Thrift Storage, Elasticsearch Index

## About Cassandra and Elasticsearch

[Apache Cassandra](http://cassandra.apache.org/) is a distributed database
designed for scalability and high availability. Cassandra supports two
protocols for communications, Thrift (legacy RPC protocol) and CQL (native
protocol). Depending on the Cassandra version, Thrift may not be started by
default. Make sure that [Thrift is started](http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsStatusThrift.html)
when using this example.

[Elasticsearch](https://www.elastic.co/products/elasticsearch) is a scalable,
distributed search engine.

> Check the JanusGraph [version compatibility](http://docs.janusgraph.org/latest/version-compat.html)
to ensure you select versions of Cassandra and Elasticsearch compatible with
this JanusGraph release.

## JanusGraph configuration

* [`jgex-cassandra.properties`](conf/jgex-cassandra.properties) contains the
Cassandra and Elasticsearch server locations. By providing different values
for `storage.cassandra.keyspace` and `index.jgex.index-name`, you can store
multiple graphs on the same Cassandra and Elasticsearch servers. Refer to
the JanusGraph [configuration reference](http://docs.janusgraph.org/latest/config-ref.html)
for additional properties.

* [`logback.xml`](conf/logback.xml) configures logging with [Logback](https://logback.qos.ch/),
which is the logger used by Cassandra. The example configuration logs to the
console and adjusts the logging level for some noisier packages. Refer to
the Logback [manual](https://logback.qos.ch/manual/index.html) for additional
details.

## Run the example

Use [Apache Maven](http://maven.apache.org/) and the [exec-maven-plugin](http://www.mojohaus.org/exec-maven-plugin/java-mojo.html)
to pull in the required jar files onto the runtime classpath.

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-cassandra

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dlogback.configurationFile="conf/logback.xml" -Dexec.args="conf/jgex-cassandra.properties"
```

## Drop the graph

Make sure to stop the application before dropping the graph.

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-cassandra

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dlogback.configurationFile="conf/logback.xml" -Dexec.args="conf/jgex-cassandra.properties drop"
```
