# Changelog

## Version Compatibility

The JanusGraph project is growing along with the rest of the graph and
big data ecosystem and utilized storage and indexing backends. Below are
version compatibilities between the various versions of components. For
dependent backend systems, different minor versions are typically
supported as well. It is strongly encouraged to verify version
compatibility prior to deploying JanusGraph.

Although JanusGraph may be compatible with older and no longer supported
versions of its dependencies, users are warned that there are possible
risks and security exposures with running software that is no longer
supported or updated. Please check with the software providers to
understand their supported versions. Users are strongly encouraged to
use the latest versions of the software.

### Version Compatibility Matrix

#### Currently supported
All currently supported verions of JanusGraph are listed below. 

!!! info
    You are currently viewing the documentation page of JanusGraph version {{ latest_version }}. To ensure that the information below is up to date, please double check that this is not an archived version of the documentation.

| JanusGraph | Storage Version | Cassandra | HBase | Bigtable | Elasticsearch | Solr | TinkerPop | Spark | Scala |
| ----- | ---- | ---- | ---- | ---- | ---- | ---- | --- | ---- | ---- |
| 0.5.z | 2 | 2.1.z, 2.2.z, 3.0.z, 3.11.z | 1.2.z, 1.3.z, 1.4.z, 2.1.z | 1.3.0, 1.4.0, 1.5.z, 1.6.z, 1.7.z, 1.8.z, 1.9.z, 1.10.z, 1.11.z, 1.14.z | 6.y, 7.y | 7.y | 3.4.z | 2.2.z | 2.11.z | 
| 0.6.z | 2 | 3.0.z, 3.11.z | 1.6.z, 2.2.z | 1.3.0, 1.4.0, 1.5.z, 1.6.z, 1.7.z, 1.8.z, 1.9.z, 1.10.z, 1.11.z, 1.14.z | 6.y, 7.y | 7.y, 8.y | 3.5.z | 3.0.z | 2.12.z |

#### End-of-Life
The versions of JanusGraph listed below are outdated and will no longer receive bugfixes.

| JanusGraph | Storage Version | Cassandra | HBase | Bigtable | Elasticsearch | Solr | TinkerPop | Spark | Scala |
| ----- | ---- | ---- | ---- | ---- | ---- | ---- | --- | ---- | ---- |
| 0.1.z| 1| 1.2.z, 2.0.z, 2.1.z| 0.98.z, 1.0.z, 1.1.z, 1.2.z| 0.9.z, 1.0.0-preZ, 1.0.0| 1.5.z| 5.2.z| 3.2.z| 1.6.z| 2.10.z| 
| 0.2.z | 1 | 1.2.z, 2.0.z, 2.1.z, 2.2.z, 3.0.z, 3.11.z | 0.98.z, 1.0.z, 1.1.z, 1.2.z, 1.3.z | 0.9.z, 1.0.0-preZ, 1.0.0 | 1.5-1.7.z, 2.3-2.4.z, 5.y, 6.y | 5.2-5.5.z, 6.2-6.6.z, 7.y | 3.2.z | 1.6.z | 2.10.z | 
| 0.3.z | 2 | 1.2.z, 2.0.z, 2.1.z, 2.2.z, 3.0.z, 3.11.z | 1.0.z, 1.1.z, 1.2.z, 1.3.z, 1.4.z | 1.0.0, 1.1.0, 1.1.2, 1.2.0, 1.3.0, 1.4.0 | 1.5-1.7.z, 2.3-2.4.z, 5.y, 6.y |  5.2-5.5.z, 6.2-6.6.z, 7.y | 3.3.z | 2.2.z | 2.11.z |
| 0.4.z | 2 | 2.1.z, 2.2.z, 3.0.z, 3.11.z | 1.2.z, 1.3.z, 1.4.z, 2.1.z | N/A | 5.y, 6.y | 7.y | 3.4.z | 2.2.z | 2.11.z |

## Release Notes

### Version 0.6.0 (Release Date: September 3, 2021)

```xml tab='Maven'
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-core</artifactId>
    <version>0.6.0</version>
</dependency>
```

```groovy tab='Gradle'
compile "org.janusgraph:janusgraph-core:0.6.0"
```

**Tested Compatibility:**

* Apache Cassandra 3.0.14, 3.11.10
* Apache HBase 1.6.0, 2.2.7
* Google Bigtable 1.3.0, 1.4.0, 1.5.0, 1.6.0, 1.7.0, 1.8.0, 1.9.0, 1.10.0, 1.11.0, 1.14.0
* Oracle BerkeleyJE 7.5.11
* Elasticsearch 6.0.1, 6.6.0, 7.14.0
* Apache Lucene 8.9.0
* Apache Solr 7.7.2, 8.9.0
* Apache TinkerPop 3.5.1
* Java 1.8

#### Changes

For more information on features and bug fixes in 0.6.0, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/17?closed=1>

#### Assets

* [JavaDoc](https://javadoc.io/doc/org.janusgraph/janusgraph-core/0.6.0)
* [GitHub Release](https://github.com/JanusGraph/janusgraph/releases/tag/v0.6.0)
* [JanusGraph zip](https://github.com/JanusGraph/janusgraph/releases/download/v0.6.0/janusgraph-0.6.0.zip)
* [JanusGraph zip with embedded Cassandra and ElasticSearch](https://github.com/JanusGraph/janusgraph/releases/download/v0.6.0/janusgraph-full-0.6.0.zip)

#### Upgrade Instructions

##### Experimental support for Amazon Keyspaces

[Amazon Keyspaces](https://aws.amazon.com/keyspaces/) is a serverless managed Apache Cassandra-compatible
database service provided by Amazon. See [Deploying on Amazon Keyspaces](https://docs.janusgraph.org/storage-backend/cassandra/#deploying-on-amazon-keyspaces-experimental)
for more details.

##### Breaking change for Configuration objects

Prior to JanusGraph 0.6.0, `Configuration` objects were from the Apache `commons-configuration` library.
To comply with the [TinkerPop change](http://tinkerpop.apache.org/docs/3.5.0-SNAPSHOT/upgrade/#_versions_and_dependencies),
JanusGraph now uses the `commons-configuration2` library. A typical usage of configuration object is to
create configuration using `ConfigurationGraphFactory`. Now you would need to use the new configuration2 library.
Please refer to the
[commons-configuration 2.0 migration guide](https://commons.apache.org/proper/commons-configuration/userguide/upgradeto2_0.html)
for details. Note that this very likely does not affect gremlin console usage, since the new library
is auto-imported, and the basic APIs remain the same. For java code usage, you need to import
configuration2 library rather than the old configuration library.

##### Breaking change for gremlin server configs

`scriptEvaluationTimeout` is renamed to `evaluationTimeout`. You can refer to `conf/gremlin-server/gremlin-server.yaml`
for example.

##### Breaking change for gremlin EventStrategy usage

If you are using [EventStrategy](https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/process/traversal/strategy/decoration/EventStrategy.html),
please note that now you need to register it every time you start a new transaction.
An example is available at [ThreadLocalTxLeakTest::eventListenersCanBeReusedAcrossTx](https://github.com/JanusGraph/janusgraph/blob/master/janusgraph-test/src/test/java/org/janusgraph/core/ThreadLocalTxLeakTest.java)
See more background of this breaking change in this
[pull request](https://github.com/JanusGraph/janusgraph/pull/2472).

##### Disable smart-limit by default and change HARD_MAX_LIMIT

Prior to 0.6.0, `smart-limit` is enabled by default. It tries to guess a small limit
for each graph centric query (e.g. `g.V().has("prop", "value")`) internally, and if more
results are required by user, it queries backend again with a larger limit, and repeats
until either results are exhausted or user stops the query. However, this is not the
same as paging mechanism. All interim results will be fetched again in next round, making
the whole query costly. Even worse, if your data backend does not return results in a consistent order,
then some entries might be missing in the final results. Until JanusGraph can fully utilize
the paging capacity provided by backends (e.g. Elasticsearch scroll), this option is
recommended to be turned off. The exception is when you have a large number of results
but you only need a few of them, then enabling `smart-limit` can reduce latency and memory
usage. An example would be:

```
Iterator<Vertex> iter = graph.traversal().V().has("prop", "value");
while (iter.hasNext()) {
    Vertex v = iter.next();
    if (canStop()) break;
}
```

Prior to 0.6.0, even if `smart-limit` is disabled, JanusGraph adds a `HARD_MAX_LIMIT` that
is equivalent to 100,000 to avoid fetching too many results at a time. This limit is now
configurable, and by default, it's Integer.MAX_VALUE which can be interpreted as no limit.

##### Add experimental support for Java 11

We started to work on support for Java 11. We would like to get feedback,
if everything is working as expected after upgrading to Java 11.

##### Removal of LoggingSchemaMaker

The `schema.default=logging` option is not valid anymore. Use `schema.default=default`
and `schema.logging=true` options together to make application behaviour unaltered,
if you are using `LoggingSchemaMaker`.

##### Replacing the server startup script is replaced

The `gremlin-server.sh` is placed by `janusgraph-server.sh`. The 
`janusgraph-server.sh` brings some new functionality such as easy 
configuration of Java options using the `jvm.options` file.

The `jvm.options` file contains some default configurations for JVM based 
on Cassandra's JVM configurations, Elasticsearch and the old gremlin-server.sh.

##### Serialization of JanusGraph predicates has changed

The serialization of JanusGraph predicates has changed in this version for both 
GraphSON and Gryo. The newest version of the JanusGraph Driver requires a JanusGraph 
Server version of 0.6.0 and above. The server includes a fallback for clients with an 
older driver to make the upgrade to version 0.6.0 easier. This means that the server 
can be upgraded first without having to update all clients at the same time. The 
fallback will however be removed in a future version of JanusGraph so clients should 
also be upgraded.

##### GraphBinary is now supported

[GraphBinary](http://tinkerpop.apache.org/docs/current/dev/io/#graphbinary) is a 
new binary serialization format from TinkerPop that supersedes Gryo and it will 
eventually also replace GraphSON. GraphBinary is language independent and has a 
low serialization overhead which results in an improved performance.

If you want to use GraphBinary, you have to add following to the `gremlin-server.yaml` 
after the keyword `serializers`. This will add the support on the server site. 

```yaml
    - { className: org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1, 
        config: { ioRegistries: [org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] }}
    - { className: org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1, 
        config: { serializeResultToString: true }}
```
!!! note 
    The java driver is the only driver that currently supports GraphBinary, 
    see [Connecting to JanusGraph using Java](interactions/connecting/java.md).

##### New index selection algorithm
In version 0.6.0, the index selection algorithm has changed. If the number of possible
indexes for a query is small enough, the new algorithm will perform an exhaustive search
to minimize the number of indexes which need to be queried. The default limit is set to 10.
In order to maintain the old selection algorithm regardless of the available indexes, set
the key `query.index-select-threshold` to `0`.
For more information, see [Configuration Reference](configs/configuration-reference.md#query)
    
##### Removal of Cassandra Thrift support

Thrift will be completely removed in Cassandra 4.
All deprecated Cassandra Thrift backends were removed in JanusGraph 0.6.0.
We already added support for CQL in JanusGraph 0.2.0 and we have been 
encouraging users to switch from Thrift to CQL since version 0.2.1.

This means that the following backends were removed: 
`cassandrathrift`, `cassandra`, `astyanax`, and `embeddedcassandra`.
Users who still use one of these Thrift backends should migrate to CQL.
[Our migration guide](operations/migrating-thrift.md) explains the 
necessary steps for this. The option to run Cassandra embedded 
in the same JVM as JanusGraph is however no longer supported with CQL.

!!! note 
    The source code for the Thrift backends will be moved into a 
    [dedicated repository](https://github.com/JanusGraph/janusgraph-cassandra).
    While we do not support them any more, users can still use them 
    if they for some reason cannot migrate to CQL.

##### Drop support for Cassandra 2

With the release of Cassandra 4, the support of Cassandra 2 will be dropped. 
Therefore, you should upgrade to Cassandra 3 or higher.

!!! note
    Cassandra 3 and higher doesn't support compact storage. If you have activated 
    or never changed the value of `storage.cql.storage-compact=true`, during the 
    upgrade process you have to ensure your data is correctly migrated.

##### Introduction of a JanusGraph Server startup class as a replacement for Gremlin Server startup

The `gremlin-server.sh` and the `janusgraph.sh` are configured to use the new JanusGraph startup class. 
This new class introduces a default set of TinkerPop Serializers if no serializers are configured in 
the `gremlin-server.yaml`. Furthermore, JanusGraph will log the version of JanusGraph and TinkerPop 
after a shiny new JanusGraph header.

!!! note
    If you have a custom script to startup JanusGraph, you propably would like to replace the Gremlin Server 
    class with JanusGraph Server class: 
    
    `org.apache.tinkerpop.gremlin.server.GremlinServer` => `org.janusgraph.graphdb.server.JanusGraphServer`

##### Drop support for Ganglia metrics

We are dropping Ganglia as we are using dropwizard for metrics. Dropwizard did drop Ganglia in the newest major version.

##### DataStax cassandra driver upgrade from 3.9.0 to 4.13.0

All DataStax cassandra driver metrics are now disabled by default. To enable DataStax driver metrics you need to provide 
a list of Session level metrics and / or Node level metrics you want to enable. To provide a list of enabled metrics, 
you can use the next configuration options: `storage.cql.metrics.session-enabled` and `storage.cql.metrics.node-enabled`. 
Notice, DataStax metrics are enabled only when basic metrics are enabled (i.e. `metrics.enabled = true`).
See configuration references `storage.cql.metrics` for additional DataStax metrics configuration.

An example configuration which enables some CQL Session level and Node level metrics reporting by JMX:
```properties
metrics.enabled=true
metrics.jmx.enabled=true
metrics.jmx.domain=com.datastax.oss.driver
metrics.jmx.agentid=agent
storage.cql.metrics.session-enabled=bytes-sent,bytes-received,connected-nodes,cql-requests,throttling.delay
storage.cql.metrics.node-enabled=pool.open-connections,pool.available-streams,bytes-sent,cql-messages
```

See `advanced.metrics.session.enabled` and `advanced.metrics.node.enabled` sections in 
[DataStax Metrics Configuration](https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/configuration/reference/) 
for a complete list of available Session level and Node level metrics.

Due to driver upgrade the next cql configuration options have been removed:

* `local-core-connections-per-host`
* `remote-core-connections-per-host`
* `local-max-requests-per-connection`
* `remote-max-requests-per-connection`
* `cluster-name`

`storage.connection-timeout` is now used to control initial connection timeout to CQL storage and not request timeouts. 
Please, use `storage.cql.request-timeout` to configure request timeouts instead.

New cql configuration options should be used for upgrade:

* `max-requests-per-connection`
* `session-name`

`storage.cql.local-datacenter` is mandatory now and defaults to `datacenter1`.

See more new cql configuration options in configuration references under `storage.cql` section.

##### Automatic configurations of dynamic graph binding

If the JanusGraphManager is configured, dynamic graph binding will be setup automatically, 
see [Dynamic Graphs](operations/dynamic-graphs.md).

!!! note 
    Breaking changes in the config of the `gremlin-server.yaml`.

Following, classes are removed and have to be replaced by tinkerpop equivalent:

| removed class | replacement class|
| --- | --- |
| `org.janusgraph.channelizers.JanusGraphWebSocketChannelizer` | `org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer` |
| `org.janusgraph.channelizers.JanusGraphHttpChannelizer` | `org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer` |
| `org.janusgraph.channelizers.JanusGraphNioChannelizer` | `org.apache.tinkerpop.gremlin.server.channel.NioChannelizer` |
| `org.janusgraph.channelizers.JanusGraphWsAndHttpChannelizer` | `org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer` |

##### Breaking change Lucene and Solr fuzzy predicates

The text predicates `text.textFuzzy` and `text.textContainsFuzzy` have been updated in both the Lucene and Solr indexing
backends to align with JanusGraph and Elastic. These predicates now inspect the query length to determine the Levenshtein
distance, where previously they used the backend's default max distance of 2:

- 0 for strings of one or two characters (exact match)
- 1 for strings of three, four or five characters
- 2 for strings of more than five characters

**Change Matrix:**

| text | query | previous result | new result |
| --- | --- | --- | --- |
| ah | ah | true | true |
| ah | ai | true | **false** |
| hop | hop | true | true |
| hop | hap | true | true |
| hop | hoop | true | true |
| hop | hooop | true | **false** |
| surprises | surprises | true | true |
| surprises | surprizes | true | true |
| surprises | surpprises | true | true |
| surprises | surpprisess | false | false |


### Version 0.5.3 (Release Date: December 24, 2020)

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.5.3</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.5.3"
    ```

**Tested Compatibility:**

* Apache Cassandra 2.2.10, 3.0.14, 3.11.0
* Apache HBase 1.2.6, 1.3.1, 1.4.10, 2.1.5
* Google Bigtable 1.3.0, 1.4.0, 1.5.0, 1.6.0, 1.7.0, 1.8.0, 1.9.0, 1.10.0, 1.11.0, 1.14.0
* Oracle BerkeleyJE 7.5.11
* Elasticsearch 6.0.1, 6.6.0, 7.6.2
* Apache Lucene 7.0.0
* Apache Solr 7.0.0
* Apache TinkerPop 3.4.6
* Java 1.8

#### Changes

For more information on features and bug fixes in 0.5.3, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/20?closed=1>

#### Assets

* [JavaDoc](https://javadoc.io/doc/org.janusgraph/janusgraph-core/0.5.3)
* [GitHub Release](https://github.com/JanusGraph/janusgraph/releases/tag/v0.5.3)
* [JanusGraph zip](https://github.com/JanusGraph/janusgraph/releases/download/v0.5.3/janusgraph-0.5.3.zip)
* [JanusGraph zip with embedded Cassandra and ElasticSearch](https://github.com/JanusGraph/janusgraph/releases/download/v0.5.3/janusgraph-full-0.5.3.zip)

### Version 0.5.2 (Release Date: May 3, 2020)

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.5.2</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.5.2"
    ```

**Tested Compatibility:**

* Apache Cassandra 2.2.10, 3.0.14, 3.11.0
* Apache HBase 1.2.6, 1.3.1, 1.4.10, 2.1.5
* Google Bigtable 1.3.0, 1.4.0, 1.5.0, 1.6.0, 1.7.0, 1.8.0, 1.9.0, 1.10.0, 1.11.0, 1.14.0
* Oracle BerkeleyJE 7.5.11
* Elasticsearch 6.0.1, 6.6.0, 7.6.2
* Apache Lucene 7.0.0
* Apache Solr 7.0.0
* Apache TinkerPop 3.4.6
* Java 1.8

For more information on features and bug fixes in 0.5.2, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/19?closed=1>

#### Upgrade Instructions

##### ElasticSearch index store names cache now enabled for any amount of indexes per store

In JanusGraph version `0.5.0` and `0.5.1` all ElasticSearch index store names are cached for efficient index store name 
retrieval and the cache is disabled if there are more than `50000` indexes available per index store. 
From JanusGraph version `0.5.2` index store names cache isn't limited to `50000` but instead can be disabled by using 
a new added parameter `enable_index_names_cache`. It is still recommended to disable index store names cache if more 
than `50000` indexes are used per index store.

### Version 0.5.1 (Release Date: March 25, 2020)

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.5.1</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.5.1"
    ```

**Tested Compatibility:**

* Apache Cassandra 2.2.10, 3.0.14, 3.11.0
* Apache HBase 1.2.6, 1.3.1, 1.4.10, 2.1.5
* Google Bigtable 1.3.0, 1.4.0, 1.5.0, 1.6.0, 1.7.0, 1.8.0, 1.9.0, 1.10.0, 1.11.0, 1.14.0
* Oracle BerkeleyJE 7.5.11
* Elasticsearch 6.0.1, 6.6.0, 7.6.1
* Apache Lucene 7.0.0
* Apache Solr 7.0.0
* Apache TinkerPop 3.4.6
* Java 1.8

For more information on features and bug fixes in 0.5.1, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/18?closed=1>

#### Upgrade Instructions

##### Two Distributed package is splitted into two version

The default version of the distribution package does no longer contain the `janusgraph.sh`. 
This includes a packaged version of cassandra and elasticsearch. If you want to have `janusgraph.sh`, 
you have to download distribution with the suffix `-full`.

##### Gremlin Server distributed with the release uses inmemory storage backend and no search backend by default

Gremlin Server is by default configured for the inmemory storage backend and no search backend when started with 
`bin/gremlin-server.sh`.  
You can provide configuration for another storage backend and/or search backend by providing a path to the appropriate 
configuration as a second parameter (`./bin/gremlin-server.sh ./conf/gremlin-server/[...].yaml`).

### Version 0.5.0 (Release Date: March 10, 2020)

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.5.0</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.5.0"
    ```

**Tested Compatibility:**

* Apache Cassandra 2.2.10, 3.0.14, 3.11.0
* Apache HBase 1.2.6, 1.3.1, 1.4.10, 2.1.5
* Google Bigtable 1.3.0, 1.4.0, 1.5.0, 1.6.0, 1.7.0, 1.8.0, 1.9.0, 1.10.0, 1.11.0
* Oracle BerkeleyJE 7.5.11
* Elasticsearch 6.0.1, 6.6.0, 7.6.1
* Apache Lucene 7.0.0
* Apache Solr 7.0.0
* Apache TinkerPop 3.4.6
* Java 1.8

For more information on features and bug fixes in 0.5.0, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/13?closed=1>

#### Upgrade Instructions

##### Distributed package is renamed

The distribution has no longer the suffix `-hadoop2`.

##### Reorder dependency of Hadoop

Hadoop is now a dependency of supported backends. Therefore, `MapReduceIndexJobs` is now split up into different classes:

| Old Function | New Function |
| ------------ | ------------ |
|`MapReduceIndexJobs.cassandraRepair`|`CassandraMapReduceIndexJobsUtils.repair`| 
|`MapReduceIndexJobs.cassandraRemove`|`CassandraMapReduceIndexJobsUtils.remove`|
|`MapReduceIndexJobs.cqlRepair`|`CqlMapReduceIndexJobsUtils.repair`| 
|`MapReduceIndexJobs.cqlRemove`|`CqlMapReduceIndexJobsUtils.remove`| 
|`MapReduceIndexJobs.hbaseRepair`|`HBaseMapReduceIndexJobsUtils.repair`| 
|`MapReduceIndexJobs.hbaseRemove`|`HBaseMapReduceIndexJobsUtils.remove`| 

!!! note
    Now, you can easily support for any backend.

!!! warning
    `Cassandra3InputFormat` is replaced by `CqlInputFormat`

##### ElasticSearch: Upgrade from 6.6.0 to 7.6.1 and drop support for 5.x version
The ElasticSearch version has been changed to 7.6.1 which removes support for `max-retry-timeout` option. 
That is why this option no longer available in JanusGraph.
Users should be aware that by default JanusGraph setups maximum open scroll contexts to maximum value of `2147483647` with the parameter `setup-max-open-scroll-contexts` for ElasticSearch 7.y. 
This option can be disabled and updated manually in ElasticSearch
but you should be aware that ElasticSearch starting from version 7 has a default limit of 500 opened contexts 
which most likely be reached by the normal usage of JanusGraph with ElasticSearch.
By default deprecated mappings are disabled in ElasticSearch version 7. 
If you are upgrading your ElasticSearch index backend to version 7 from lower versions, 
it is recommended to reindex your JanusGraph indices to not use mappings. 
If you are unable to reindex your indices you may setup parameter `use-mapping-for-es7` to `true` 
which will tell JanusGraph to use mapping types for ElasticSearch version 7.
Due to the drop of support for 5.x version, deprecated multi-type indices are no more supported. 
Parameter `use-deprecated-multitype-index` is no more supported by JanusGraph.

##### BerkeleyDB

BerkeleyDB storage configured with [SHARED_CACHE](https://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/EnvironmentConfig.html#SHARED_CACHE) for better memory usage.

##### Default logging location has changed

If you are using `janusgraph.sh` to start your instance, the default logging has been changed from `log` to `logs`

### Version 0.4.1 (Release Date: January 14, 2020)

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.4.1</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.4.1"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 1.2.6, 1.3.1, 1.4.10, 2.1.5
-   Google Bigtable 1.3.0, 1.4.0, 1.5.0, 1.6.0, 1.7.0, 1.8.0, 1.9.0, 1.10.0, 1.11.0
-   Oracle BerkeleyJE 7.5.11
-   Elasticsearch 5.6.14, 6.0.1, 6.6.0
-   Apache Lucene 7.0.0
-   Apache Solr 7.0.0
-   Apache TinkerPop 3.4.4
-   Java 1.8

For more information on features and bug fixes in 0.4.1, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/15?closed=1>

#### Upgrade Instructions

##### TinkerPop: Upgrade from 3.4.1 to 3.4.4

Adding multiple values in the same query to a new vertex property without explicitly defined type 
(i.e. using `Automatic Schema Maker` to create a property type) requires explicit usage of `VertexProperty.Cardinality` 
for each call (only for the first query which defines a property) if the `VertexProperty.Cardinality` is different than 
`VertexProperty.Cardinality.single`.

### Version 0.4.0 (Release Date: July 1, 2019)
Legacy documentation: <https://old-docs.janusgraph.org/0.4.0/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.4.0</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.4.0"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 1.2.6, 1.3.1, 1.4.10, 2.1.5
-   Google Bigtable 1.3.0, 1.4.0, 1.5.0, 1.6.0, 1.7.0, 1.8.0, 1.9.0, 1.10.0, 1.11.0
-   Oracle BerkeleyJE 7.5.11
-   Elasticsearch 5.6.14, 6.0.1, 6.6.0
-   Apache Lucene 7.0.0
-   Apache Solr 7.0.0
-   Apache TinkerPop 3.4.1
-   Java 1.8

For more information on features and bug fixes in 0.4.0, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/8?closed=1>

#### Upgrade Instructions

##### HBase: Upgrade from 1.2 to 2.1
The version of HBase that is included in the distribution of JanusGraph was upgraded from 1.2.6 to 2.1.5.
HBase 2.x client is not fully backward compatible with HBase 1.x server. Users who operate their own HBase version 1.x cluster may need to upgrade their cluster to version 2.x.
Optionally users may build their own distribution of JanusGraph which includes HBase 1.x from source with the maven flags -Dhbase.profile -Phbase1.

##### Cassandra: Upgrade from 2.1 to 2.2
The version of Cassandra that is included in the distribution of JanusGraph was upgraded from 2.1.20 to 2.2.13.
Refer to [the upgrade documentation of Cassandra](https://github.com/apache/cassandra/blob/174cf761f7897443080b8a840b649b7eab17ae25/NEWS.txt#L787)
for detailed instructions to perform this upgrade.
Users who operate their own Cassandra cluster instead of using Cassandra distributed together with JanusGraph are not affected by this upgrade.
This also does not change the different versions of Cassandra that are supported by JanusGraph (see <<version-compat>> for a detailed list of the supported versions).

##### BerkeleyDB : Upgrade from 7.4 to 7.5
The BerkeleyDB version has been updated, and it contains changes to the file format stored on disk 
(see [the BerkeleyDB changelog for reference](https://docs.oracle.com/cd/E17277_02/html/changelog.html)).
This file format change is forward compatible with previous versions of BerkeleyDB, so existing graph data stored with JanusGraph can be read in.
However, once the data has been read in with the newer version of BerkeleyDB, those files can no longer be read by the older version.
Users are encouraged to backup the BerkeleyDB storage directory before attempting to use it with the JanusGraph release.

##### Solr: Compatible Lucene version changed from 5.0.0 to 7.0.0 in distributed config
The JanusGraph distribution contains a `solrconfig.xml` file that can be used to configure Solr.
The value `luceneMatchVersion` in this config that tells Solr to behave according to that Lucene version was changed from 5.0.0 to 7.0.0 as that is the default version currently used by JanusGraph.
Users should generally set this value to the version of their Solr installation.
If the config distributed by JanusGraph is used for an existing Solr installation that used a lower version before (like 5.0.0 from a previous versions of this file), it is highly recommended that a re-indexing is performed.

### Version 0.3.3 (Release Date: January 11, 2020)

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.3.3</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.3.3"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.20, 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 1.2.6, 1.3.1, 1.4.4
-   Google Bigtable 1.0.0, 1.1.2, 1.2.0, 1.3.0, 1.4.0
-   Oracle BerkeleyJE 7.4.5
-   Elasticsearch 1.7.6, 2.4.6, 5.6.5, 6.0.1
-   Apache Lucene 7.0.0
-   Apache Solr 5.5.4, 6.6.1, 7.0.0
-   Apache TinkerPop 3.3.3
-   Java 1.8

For more information on features and bug fixes in 0.3.3, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/14?closed=1>

### Version 0.3.2 (Release Date: June 16, 2019)
Legacy documentation: <https://old-docs.janusgraph.org/0.3.2/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.3.2</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.3.2"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.20, 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 1.2.6, 1.3.1, 1.4.4
-   Google Bigtable 1.0.0, 1.1.2, 1.2.0, 1.3.0, 1.4.0
-   Oracle BerkeleyJE 7.4.5
-   Elasticsearch 1.7.6, 2.4.6, 5.6.5, 6.0.1
-   Apache Lucene 7.0.0
-   Apache Solr 5.5.4, 6.6.1, 7.0.0
-   Apache TinkerPop 3.3.3
-   Java 1.8

For more information on features and bug fixes in 0.3.2, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/10?closed=1>

### Version 0.3.1 (Release Date: October 2, 2018)
Legacy documentation: <https://old-docs.janusgraph.org/0.3.1/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.3.1</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.3.1"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.20, 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 1.2.6, 1.3.1, 1.4.4
-   Google Bigtable 1.0.0, 1.1.2, 1.2.0, 1.3.0, 1.4.0
-   Oracle BerkeleyJE 7.4.5
-   Elasticsearch 1.7.6, 2.4.6, 5.6.5, 6.0.1
-   Apache Lucene 7.0.0
-   Apache Solr 5.5.4, 6.6.1, 7.0.0
-   Apache TinkerPop 3.3.3
-   Java 1.8

For more information on features and bug fixes in 0.3.1, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/7?closed=1>

### Version 0.3.0 (Release Date: July 31, 2018)
Legacy documentation: <https://old-docs.janusgraph.org/0.3.0/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.3.0</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.3.0"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.20, 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 1.2.6, 1.3.1, 1.4.4
-   Google Bigtable 1.0.0, 1.1.2, 1.2.0, 1.3.0, 1.4.0
-   Oracle BerkeleyJE 7.4.5
-   Elasticsearch 1.7.6, 2.4.6, 5.6.5, 6.0.1
-   Apache Lucene 7.0.0
-   Apache Solr 5.5.4, 6.6.1, 7.0.0
-   Apache TinkerPop 3.3.3
-   Java 1.8

For more information on features and bug fixes in 0.3.0, see the GitHub milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/4?closed=1>


#### Upgrade Instructions

!!! important
    You should back-up your data prior to attempting an upgrade! Also please note that once an upgrade has been completed you will no longer be able to connect to your graph with client versions prior to 0.3.0.

JanusGraph 0.3.0 implements [Schema Constraints](./schema/index.md#schema-constraints) which made it necessary to also introduce the concept of a schema version. There is a check to prevent client connections that either expect a different schema version or have no concept of a schema version. To perform an upgrade, the configuration option `graph.allow-upgrade=true` must be set on each graph you wish to upgrade. The graph must be opened with a 0.3.0 or greater version of JanusGraph since older versions have no concept of `graph.storage-version` and will not allow for it to be set.

Example excerpt from `janusgraph.properties` file
```properties
# JanusGraph configuration sample: Cassandra over a socket
#
# This file connects to a Cassandra daemon running on localhost via
# Thrift.  Cassandra must already be started before starting JanusGraph
# with this file.

# This option should be removed as soon as the upgrade is complete. Otherwise if this file
# is used in the future to connect to a different graph it could cause an unintended upgrade.
graph.allow-upgrade=true

gremlin.graph=org.janusgraph.core.JanusGraphFactory

# The primary persistence provider used by JanusGraph.  This is required.
# It should be set one of JanusGraph's built-in shorthand names for its
# standard storage backends (shorthands: berkeleyje, cassandrathrift,
# cassandra, astyanax, embeddedcassandra, cql, hbase, inmemory) or to the
# full package and classname of a custom/third-party StoreManager
# implementation.
#
# Default:    (no default value)
# Data Type:  String
# Mutability: LOCAL
storage.backend=cassandrathrift

# The hostname or comma-separated list of hostnames of storage backend
# servers.  This is only applicable to some storage backends, such as
# cassandra and hbase.
#
# Default:    127.0.0.1
# Data Type:  class java.lang.String[]
# Mutability: LOCAL
storage.hostname=127.0.0.1
```

If `graph.allow-upgrade` is set to true on a graph `graph.storage-version` and `graph.janusgraph-version` will automatically be upgraded to match the version level of the server, or local client, that is opening the graph.
You can verify the upgrade was successful by opening the management API and validating the values of `graph.storage-version` and `graph.janusgraph-version`.

Once the storage version has been set you should remove `graph.allow-upgrade=true` from your properties file and reopen your graph to ensure that the upgrade was successful. 

### Version 0.2.3 (Release Date: May 21, 2019)
Legacy documentation: <https://old-docs.janusgraph.org/0.2.3/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.2.3</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.2.3"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.20, 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 0.98.24-hadoop2, 1.2.6, 1.3.1
-   Google Bigtable 1.0.0
-   Oracle BerkeleyJE 7.3.7
-   Elasticsearch 1.7.6, 2.4.6, 5.6.5, 6.0.1
-   Apache Lucene 7.0.0
-   Apache Solr 5.5.4, 6.6.1, 7.0.0
-   Apache TinkerPop 3.2.9
-   Java 1.8

For more information on features and bug fixes in 0.2.3, see the GitHub
milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/9?closed=1>

### Version 0.2.2 (Release Date: October 9, 2018)
Legacy documentation: <https://old-docs.janusgraph.org/0.2.2/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.2.2</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.2.2"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.20, 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 0.98.24-hadoop2, 1.2.6, 1.3.1
-   Google Bigtable 1.0.0
-   Oracle BerkeleyJE 7.3.7
-   Elasticsearch 1.7.6, 2.4.6, 5.6.5, 6.0.1
-   Apache Lucene 7.0.0
-   Apache Solr 5.5.4, 6.6.1, 7.0.0
-   Apache TinkerPop 3.2.9
-   Java 1.8

For more information on features and bug fixes in 0.2.2, see the GitHub
milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/6?closed=1>

### Version 0.2.1 (Release Date: July 9, 2018)
Legacy documentation: <https://old-docs.janusgraph.org/0.2.1/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.2.1</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.2.1"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.20, 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 0.98.24-hadoop2, 1.2.6, 1.3.1
-   Google Bigtable 1.0.0
-   Oracle BerkeleyJE 7.3.7
-   Elasticsearch 1.7.6, 2.4.6, 5.6.5, 6.0.1
-   Apache Lucene 7.0.0
-   Apache Solr 5.5.4, 6.6.1, 7.0.0
-   Apache TinkerPop 3.2.9
-   Java 1.8

For more information on features and bug fixes in 0.2.1, see the GitHub
milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/5?closed=1>

#### Upgrade Instructions
##### HBase TTL

In JanusGraph 0.2.0, time-to-live (TTL) support was added for HBase
storage backend. In order to utilize the TTL capability on HBase, the
graph timestamps need to be MILLI. If the `graph.timestamps` property is
not explicitly set to MILLI, the default is MICRO in JanusGraph 0.2.0,
which does not work for HBase TTL. Since the `graph.timestamps` property
is FIXED, a new graph needs to be created to make any change of the
`graph.timestamps` property effective.

### Version 0.2.0 (Release Date: October 11, 2017)
Legacy documentation: <https://old-docs.janusgraph.org/0.2.0/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.2.0</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.2.0"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.18, 2.2.10, 3.0.14, 3.11.0
-   Apache HBase 0.98.24-hadoop2, 1.2.6, 1.3.1
-   Google Bigtable 1.0.0-pre3
-   Oracle BerkeleyJE 7.3.7
-   Elasticsearch 1.7.6, 2.4.6, 5.6.2, 6.0.0-rc1
-   Apache Lucene 7.0.0
-   Apache Solr 5.5.4, 6.6.1, 7.0.0
-   Apache TinkerPop 3.2.6
-   Java 1.8

For more information on features and bug fixes in 0.2.0, see the GitHub
milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/2?closed=1>

#### Upgrade Instructions

##### Elasticsearch

JanusGraph 0.1.z is compatible with Elasticsearch 1.5.z. There were
several configuration options available, including transport client,
node client, and legacy configuration track. JanusGraph 0.2.0 is
compatible with Elasticsearch versions from 1.y through 6.y, however it
offers only a single configuration option using the REST client.

##### Transport client

The `TRANSPORT_CLIENT` interface has been replaced with `REST_CLIENT`.
When migrating an existing graph to JanusGraph 0.2.0, the `interface`
property must be set when connecting to the graph:
```properties
index.search.backend=elasticsearch
index.search.elasticsearch.interface=REST_CLIENT
index.search.hostname=127.0.0.1
```

After connecting to the graph, the property update can be made permanent
by making the change with `JanusGraphManagement`:
```groovy
mgmt = graph.openManagement()
mgmt.set("index.search.elasticsearch.interface", "REST_CLIENT")
mgmt.commit()
```

##### Node client

A node client with JanusGraph can be configured in a few ways. If the
node client was configured as a client-only or non-data node, follow the
steps from the [transport client](#_transport_client) section to connect
to the existing cluster using the `REST_CLIENT` instead. If the node
client was a data node (local-mode), then convert it into a standalone
Elasticsearch node, running in a separate JVM from your application
process. This can be done by using the node’s configuration from the
JanusGraph configuration to start a standalone Elasticsearch 1.5.z node.
For example, we start with these JanusGraph 0.1.z properties:

    index.search.backend=elasticsearch
    index.search.elasticsearch.interface=NODE
    index.search.conf-file=es-client.yml
    index.search.elasticsearch.ext.node.name=alice

where the configuration file `es-client.yml` has properties:

    node.data: true
    path.data: /var/lib/elasticsearch/data
    path.work: /var/lib/elasticsearch/work
    path.logs: /var/log/elasticsearch

The properties found in the configuration file `es-client.yml` and the
`index.search.elasticsearch.ext.*` properties can be inserted into
`$ES_HOME/config/elasticsearch.yml` so that a standalone Elasticsearch
1.5.z node can be started with the same properties. Keep in mind that if
any `path` locations have relative paths, those values may need to be
updated appropriately. Once the standalone Elasticsearch node is
started, follow the directions in the [transport
client](#_transport_client) section to complete the migration to the
`REST_CLIENT` interface. Note that the `index.search.conf-file` and
`index.search.elasticsearch.ext.*` properties are not used by the
`REST_CLIENT` interface, so they can be removed from the configuration
properties.

##### Legacy configuration

The legacy configuration track was not recommended in JanusGraph 0.1.z
and is no longer supported in JanusGraph 0.2.0. Users should refer to
the previous sections and migrate to the `REST_CLIENT`.


### Version 0.1.1 (Release Date: May 11, 2017)
Documentation: <https://old-docs.janusgraph.org/0.1.1/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.1.1</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.1.1"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.9
-   Apache HBase 0.98.8-hadoop2, 1.0.3, 1.1.8, 1.2.4
-   Google Bigtable 0.9.5.1
-   Oracle BerkeleyJE 7.3.7
-   Elasticsearch 1.5.1
-   Apache Lucene 4.10.4
-   Apache Solr 5.2.1
-   Apache TinkerPop 3.2.3
-   Java 1.8

For more information on features and bug fixes in 0.1.1, see the GitHub
milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/3?closed=1>

### Version 0.1.0 (Release Date: April 11, 2017)
Documentation: <https://old-docs.janusgraph.org/0.1.0/index.html>

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.janusgraph</groupId>
        <artifactId>janusgraph-core</artifactId>
        <version>0.1.0</version>
    </dependency>
    ```

=== "Gradle"
    ```groovy
    compile "org.janusgraph:janusgraph-core:0.1.0"
    ```

**Tested Compatibility:**

-   Apache Cassandra 2.1.9
-   Apache HBase 0.98.8-hadoop2, 1.0.3, 1.1.8, 1.2.4
-   Google Bigtable 0.9.5.1
-   Oracle BerkeleyJE 7.3.7
-   Elasticsearch 1.5.1
-   Apache Lucene 4.10.4
-   Apache Solr 5.2.1
-   Apache TinkerPop 3.2.3
-   Java 1.8

**Features added since version Titan 1.0.0:**

-   TinkerPop 3.2.3 compatibility

    -   Includes update to Spark 1.6.1

-   Query optimizations: JanusGraphStep folds in HasId and HasContainers
    can be folded in even mid-traversal

-   Support Google Cloud Bigtable as a backend over the HBase interface

-   Compatibility with newer versions of backend and index stores

    -   HBase 1.2

    -   BerkeleyJE 7.3.7

-   Includes a number of bug fixes and optimizations

For more information on features and bug fixes in 0.1.0, see the GitHub
milestone:

-   <https://github.com/JanusGraph/janusgraph/milestone/1?closed=1>

#### Upgrade Instructions

JanusGraph is based on the latest commit to the `titan11` branch of
[Titan repo](https://github.com/thinkaurelius/titan).

JanusGraph has made the following changes to Titan, so you will need to
adjust your code and configuration accordingly:

1.  module names: `titan-*` are now `janusgraph-*`

2.  package names: `com.thinkaurelius.titan` are now `org.janusgraph`

3.  class names: `Titan*` are now `JanusGraph*` except in cases where
    this would duplicate a word, e.g., `TitanGraph` is simply
    `JanusGraph` rather than `JanusGraphGraph`

For more information on how to configure JanusGraph to read data which
had previously been written by Titan refer to [Migration from titan](operations/migrating-titan.md).
