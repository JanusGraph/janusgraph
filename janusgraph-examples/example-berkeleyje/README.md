# BerkeleyJE Storage, Lucene Index

## About BerkeleyJE and Lucene

[Oracle Berkeley DB Java Edition](https://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html)
is an embedded database, so it runs within your application rather than as
a standalone server. The data is stored in a directory on the file system.

[Apache Lucene](https://lucene.apache.org/) is an embedded index, so it runs
within your application rather than as a standalone server. The data is
stored in a directory on the file system.

## JanusGraph configuration

[`jgex-berkeleyje.properties`](conf/jgex-berkeleyje.properties) contains
the directory locations for BerkeleyJE and Lucene.

Refer to the JanusGraph [configuration reference](https://docs.janusgraph.org/basics/configuration-reference/)
for additional properties.

## Dependencies

The required Maven dependency for BerkeleyJE:

```
        <dependency>
            <groupId>org.janusgraph</groupId>
            <artifactId>janusgraph-berkeleyje</artifactId>
            <version>${janusgraph.version}</version>
            <scope>runtime</scope>
        </dependency>
```

The required Maven dependency for Lucene:

```
        <dependency>
            <groupId>org.janusgraph</groupId>
            <artifactId>janusgraph-lucene</artifactId>
            <version>${janusgraph.version}</version>
            <scope>runtime</scope>
        </dependency>
```

## Run the example

This command can be run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-berkeleyje
```

## Drop the graph

After running an example, you may want to drop the graph from storage. Make
sure to stop the application before dropping the graph. This command can be
run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-berkeleyje -Dcmd=drop
```

The configuration uses the application name `jgex` as the root directory
for the BerkeleyJE and Lucene directories. The directory is safe to remove
after running the drop command.

```
rm -rf jgex/
```
