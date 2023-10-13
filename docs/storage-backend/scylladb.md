# ScyllaDB

![](scylladb.svg)

> ScyllaDB is a NoSQL database with a close-to-the-hardware, shared-nothing approach that optimizes raw performance, fully utilizes modern multi-core servers, and minimizes the overhead to DevOps. ScyllaDB is API-compatible with both Cassandra and DynamoDB, yet is much faster, more consistent, and with a lower TCO.
>
> —  [ScyllaDB
> Homepage](https://www.scylladb.com/)

## ScyllaDB Setup and Connection

ScyllaDB is fully compatible with Cassandra. To use it as the data storage layer:

1. Spin up a Scylla cluster. You can do this using Scylla Cloud, using Docker, running Scylla in the cloud or on-prem. You can see a step-by-step guide on how to [spin up a three node Scylla cluster using Docker](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/topic/consistency-level-demo-part-1/) in this lesson.
2. Run JanusGraph with “cql” as the storage.backend. 
3. Specify the IP address of one of the Scylla nodes in your cluster as the storage.hostname.


### Step by Step Tutorial

[This Scylla University lesson provides step-by-step instructions for using JanusGraph with ScyllaDB](https://university.scylladb.com/courses/the-mutant-monitoring-system-training-course/lessons/a-graph-data-system-powered-by-scylladb-and-janusgraph/) as the data storage layer. The main steps in the lesson are:

- Spinning up a virtual machine 
- Installing the prerequisites
- Running the JanusGraph server (using Docker)
- Running a Gremlin Console to connect to the new server (also in Docker)
- Spinning up a three-node Scylla Cluster and setting it as the data storage for the JanusGraph server
- Performing some basic graph operations

## Usage of Scylla optimized driver

JanusGraph provides `scylla` `storage.backend` options in addition to generic `cql` option.  
ScyllaDB can work using any of those storage options, but the dedicated `scylla` backend is better optimized 
for ScyllaDB. Thus, it's recommended to use `scylla` backend option with ScyllaDB whenever possible.  
`scylla` storage option is included in `janusgraph-scylla` library but this library isn't shipped via `janusgraph-all` 
and this library is not part of the provided JanusGraph distributions.  
To make `scylla` option available in JanusGraph it's necessary to replace `org.janusgraph:janusgraph-cql` 
with `org.janusgraph:janusgraph-scylla` if JanusGraph is used in Embedded Mode.   

In case `janusgraph-all` dependency is used instead of explicit JanusGraph dependencies then the replacement to
`org.janusgraph:janusgraph-scylla` can be done like below.

```xml tab='Maven'
<dependencies>
    <!-- Exclude `janusgraph-cql` from `janusgraph-all` -->
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-all</artifactId>
        <version>1.0.0</version>
        <exclusions>
            <exclusion>
                <groupId>org.janusgraph</groupId>
                <artifactId>janusgraph-cql</artifactId>
            </exclusion>
        </exclusions>
    </dependency>

    <!-- Include `janusgraph-scylla` -->
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-scylla</artifactId>
        <version>1.0.0</version>
    </dependency>
</dependencies>
```

```groovy tab='Gradle'
/* Exclude `janusgraph-cql` from `janusgraph-all` */
compile("org.janusgraph:janusgraph-all:1.0.0") {
    exclude group: 'org.janusgraph', module: 'janusgraph-cql'
}
/* Include `janusgraph-scylla` */
compile "org.janusgraph:janusgraph-scylla:1.0.0"
```

#### Using Scylla driver in JanusGraph Server provided via the standard distribution builds

!!! note
    The manual process described below is temporary and should be replaced by automated process after the issue
    [janusgraph/janusgraph#3580](https://github.com/JanusGraph/janusgraph/issues/3580) is resolved.

In case `scylla` storage option is needed to be used in JanusGraph distribution then it's necessary to replace the next 
DataStax provided CQL driver with Scylla provided CQL driver.
The default JanusGraph distribution builds contain only DataStax drivers, but not Scylla drivers because they are conflicting. 
However, it's possible to build the same distribution build but with Scylla drivers enabled instead. 
To do this, you can use the following command inside the JanusGraph repository:
```groovy tab='Bash'
mvn clean install -Pjanusgraph-release -Puse-scylla -DskipTests=true --batch-mode --also-make -Dgpg.skip=true
```

This command will generate distribution builds (both normal and full distribution build) in the 
following directory: `janusgraph-dist/target/`.   

Otherwise, if you can't build distribution on your own, you can use the JanusGraph provided distribution and replace 
the following libraries in `lib` directory (all libraries can be downloaded via Maven Central Repository). 
- `org.janusgraph:janusgraph-cql` with `org.janusgraph:janusgraph-scylla`
- `org.janusgraph:cassandra-hadoop-util` with `org.janusgraph:scylla-hadoop-util`
- `com.datastax.oss:java-driver-core` with `com.scylladb:java-driver-core`
- `com.datastax.oss:java-driver-query-builder` with `com.scylladb:java-driver-query-builder`
- `com.datastax.cassandra:cassandra-driver-core` with `com.scylladb:scylla-driver-core`

The versions of `com.scylladb:java-driver-core` and `com.scylladb:java-driver-query-builder` should be equal to the 
version found in `pom.xml` of `org.janusgraph:janusgraph-scylla`. The version of `com.scylladb:scylla-driver-core` should 
be equal to the version found in `org.janusgraph:scylla-hadoop-util`.

After replacement is done it will be possible to use `scylla` as `storage.backend` options which will use the optimized 
Scylla driver.
