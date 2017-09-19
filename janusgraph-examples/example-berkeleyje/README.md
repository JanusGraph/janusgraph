# BerkeleyJE Storage, Lucene Index

## About BerkeleyJE and Lucene

[Oracle Berkeley DB Java Edition](http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html)
is an embedded database, so it runs within your application rather than as
a standalone server. By including the `janusgraph-berkeleyje` dependency,
the required jar files are pulled in. The data is stored in a directory on
the file system.

[Apache Lucene](http://lucene.apache.org/) is an embedded index, so it runs
within your application rather than as a standalone server. By including the
`janusgraph-lucene` dependency, the required jar files are pulled in. The
data is stored in a directory on the file system.

## JanusGraph configuration

[`jgex-berkeleyje.properties`](conf/jgex-berkeleyje.properties) contains
the directory locations for BerkeleyJE and Lucene.

Refer to the JanusGraph [configuration reference](http://docs.janusgraph.org/latest/config-ref.html)
for additional properties.

## Running the example

Use [Apache Maven](http://maven.apache.org/) and the
[exec-maven-plugin](http://www.mojohaus.org/exec-maven-plugin/java-mojo.html)
to pull in the required jar files onto the runtime classpath.

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-berkeleyje

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dexec.args="conf/jgex-berkeleyje.properties"
```

## Drop the graph

Make sure to stop the application before dropping the graph. The configuration
uses the application name `jgex` as the root directory for the BerkeleyJE
and Lucene directories.

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-berkeleyje

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.JanusGraphApp" -Dexec.args="conf/jgex-berkeleyje.properties drop"

$ rm -rf jgex/
```
