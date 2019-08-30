Testing JanusGraph
==================

### Audience of this Document

This page is written for developers familiar with Java, JanusGraph, and Maven who want information on how to run JanusGraph's test suite.

### Overview

JanusGraph runs all tests using JUnit.  To compile, package, and run the default test suite for JanusGraph, use the standard `mvn clean install` command.

JanusGraph has a specialty tests, disabled by default, intended to generate basic performance metrics or stress its cache structures under memory pressure.  The next section describes how JanusGraph's tests are internally categorized and the Maven options that enabled/disable test categories.

### Continuous Integration

JanusGraph runs continuous integration via Travis; see the [dashboard](https://travis-ci.org/JanusGraph/janusgraph) for current status.

Travis sends emails on test failures and status transitions (to/from failure) to
[janusgraph-ci@googlegroups.com](https://groups.google.com/forum/#!forum/janusgraph-ci) mailing list.

### JUnit Test Tags

All of JanusGraph's tests are written for JUnit.  JanusGraph's JUnit tests are annotated with the following [JUnit Tags](https://junit.org/junit5/docs/current/user-guide/#writing-tests-tagging-and-filtering):


| Category Name | Maven Property | Default | Comment |
| ------------- | ------------------- |:------------:| ------- |
| MEMORY_TESTS | test.skip.mem | true (disabled) | Tests intended to exert memory pressure |
| PERFORMANCE_TESTS | test.skip.perf | true (disabled) | Tests written as simple speed tests using JUnitBenchmarks|
| ORDERED_KEY_STORE_TESTS | test.skip.ordered | false (enabled) | Tests written for a storage backend that stores data in key order |
| UNORDERED_KEY_STORE_TESTS | test.skip.unordered | false (enabled) | Tests written for a storage backend that doesn't store data in key order |
| (No&nbsp;tag) | test.skip.default | false (enabled) | Tests without any Tag annotations |

**Tag Name** above is a Java interface defined in the package [org.janusgraph.testcategory](janusgraph-test/src/main/org/janusgraph/testcategory).  These interfaces appear as arguments to the JUnit `@Tag(...)` annotation, e.g. `@Tag(TestCategory.MEMORY_TESTS)`.

**Maven Property** above is a boolean-valued pom.xml property that skips the associated test tag when true and executes the associated test tag when false.  The default values defined in pom.xml can be overridden on the command-line in the ordinary Maven way, e.g. `mvn -Dtest.skip.mem=false test`.

*Implementation Note.*  The Maven property naming pattern "test.skip.x=boolean" is needlessly verbose, a cardinal sin for command line options.  A more concise alternative would be "test.x" with the boolean sense negated.  However, this complicates the pom.xml configuration for the Surefire plugin since it precludes direct use of the Surefire plugin's `<skip>` configuration tag, as in `<skip>${test.skip.perf}</skip>`.  There doesn't seem to be a straightforward way to negate a boolean or otherwise make this easy, at least without resorting to profiles or a custom plugin, though I might be missing something.  Also, the mold is arguably already set by Surefire's "maven.test.skip" property, though that has slightly different interpretation semantics than the properties above.

### Marking tests as flaky

If a test should be marked as flaky add following annotation to the test and open an issue.

```java
@FlakyTest
public void testFlakyFailsSometimes(){}
```

### Running a Single Test via Maven

The standard maven-surefire-plugin option applies for most tests:

```bash
mvn test -Dtest=full.or.partial.classname#methodname
```

However, MEMORY_TESTS and PERFORMANCE_TESTS are disabled by default regardless of whatever `-Dtest=...` option might be specified.  When running a single MemoryTest or PerformanceTest, specify `-Dtest.mem=true` or `-Dtest.perf=true` as appropriate for the test in question.

Here's a concrete example.

```bash
# Executes no tests because the MEMORY_TESTS category is disabled by default
mvn test -Dtest=BerkeleyJEGraphPerformanceMemoryTest
# Executes the specified test
mvn test -Dtest=BerkeleyJEGraphPerformanceMemoryTest -Dtest.skip.mem=false
```

### Running Tests with an External Solr

Solr tests can be run against an external Solr instance. For convenience the `docker` Maven profile is provided to manage a Solr Docker container through the Maven Failsafe Plugin. The default test version will be the same as the Solr client version.

```bash
mvn clean install -pl janusgraph-solr -Pdocker
```

Additional Maven profiles are defined for testing against default versions of other supported major Solr releases.
(Currently, only Solr 7 is supported.)

```bash
mvn clean install -pl janusgraph-solr -Pdocker,solr7
```

Finally the `solr.test.version` property can be used to test against arbitrary Solr versions.

```bash
mvn clean install -pl janusgraph-solr -Pdocker -Dsolr.test.version=7.0.0
```

### Running Elasticsearch Tests

**Note** Running Elasticsearch tests require Docker.

Elasticsearch tests run against an external Elasticsearch instance. The default test version will be the same as the Elasticsearch client version.

```bash
mvn clean install -pl janusgraph-es
```

Additional Maven profiles are defined for testing against default versions of other supported major Elasticsearch releases.

```bash
mvn clean install -pl janusgraph-es -Pelasticsearch5
```

Finally the `elasticsearch.docker.version` property can be used to test against arbitrary Elasticsearch versions >= `5.0.0`. This is more complicated however because of differences across major versions in required server settings and Docker image names. The examples below illustrate the differences based on the Elasticsearch major version.

```bash
mvn clean install -pl janusgraph-es -Delasticsearch.docker.version=5.3.2
mvn clean install -pl janusgraph-es -Delasticsearch.docker.image=elasticsearch
mvn clean install -pl janusgraph-es -Delasticsearch.docker.version=6.0.0 -Delasticsearch.docker.image=elasticsearch
```

### Running CQL Tests

**Note** Running CQL tests require Docker.

CQL tests are executed using [testcontainers-java](https://www.testcontainers.org/). 
CQL tests can be executed against a Cassandra 2 using the profile `cassandra2`, 
a Cassandra 3 using the profile `cassandra3`, or a Scylla 3 using the profile `scylladb`.

```bash
mvn clean install -pl janusgraph-cql -Pcassandra2-murmur
mvn clean install -pl janusgraph-cql -Pcassandra3-murmur
mvn clean install -pl janusgraph-cql -Pscylladb
```

#### Special versions of Cassandra

System properties to configure CQL test executions:

| Property | Description | Default value |
| -------- | ----------- | ------------- |
| `cassandra.docker.image` | Docker image to pull and run. | `cassandra` |
| `cassandra.docker.version` | Docker image tag to pull and run  | `3.11.4` |
| `cassandra.docker.partitioner` | Set the cassandra partitioner. Supported partitioner are `murmur`, or `byteordered`| `murmur` |
| `cassandra.docker.useSSL` | Activate SSL **Note: This property currently only works with the partitioner set to `murmur`.** | `false` |
| `cassandra.docker.useDefaultConfigFromImage` | If set to `false` default configs of the image are used. **Note: `cassandra.docker.partitioner` and `cassandra.docker.useSSL` are ignored.** | `false` |

The following examples show possible configuration combinations.

```bash
mvn clean install -pl janusgraph-cql -Dcassandra.docker.version=2.2.14
mvn clean install -pl janusgraph-cql -Dcassandra.docker.image=cassandra
mvn clean install -pl janusgraph-cql -Dcassandra.docker.image=cassandra -Dcassandra.docker.version=3.11.2
```

#### TinkerPop tests

The CQL backend is tested with TinkerPop tests using following command. 

**Note: Profiles are not supported during running TinkerPop tests. 
If you do not want to use the default config, you can set `cassandra.docker.image`, 
`cassandra.docker.version`, or `cassandra.docker.partitioner`.**

```bash
mvn clean install -Dtest.skip.tp=false -DskipTests=true -pl janusgraph-cql \
  -Dcassandra.docker.partitioner=murmur -Dcassandra.docker.version=2.2.14
```

#### Create new configuration files for new Versions of Cassandra

The file `janusgraph-cql/src/test/resources/docker/docker-compose.yml` can be used to generate new configuration files. 
Therefore, you have to start a Cassandra instance using `docker-compose up`. 
Afterward, you can extract the configuration which is located in the following file `/etc/cassandra/cassandra.yaml`.

### Running Tests with an External Cassandra

Default and TinkerPop tests can be run against an externally-managed Cassandra instance. For convenience a Docker Compose file is provided in the JanusGraph-Cassandra source distribution for managing a Cassandra instance in a Docker container.

```bash
CASSANDRA_VERSION=3.11.0 docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
```

Environment variables used when starting the container are described below

| Variable | Description | Example |
| ---- | ---- | ---- |
| CASSANDRA_VERSION | Docker image version to pull and run | 3.11.0 |
| CASSANDRA_ENABLE_BOP | Enable the `ByteOrderedPartitioner`. Required for TinkerPop tests. | true |
| CASSANDRA_ENABLE_SSL | Enable SSL | true |

(Optional) Once the instance is started logs can be monitored or alternatively omit the `-d` flag in the above call and run tests in a separate shell

```bash
docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml logs -f
```

Wait for the instance to become available by monitoring logs or programmatically as shown below. Note the two cases being checked are to accommodate both unencrypted and encrypted connections.

```bash
until docker exec -it jg-cassandra sh -c 'exec cqlsh -e "show host"' || docker exec -it jg-cassandra sh -c 'exec cqlsh --ssl -e "show host"'; do
  >&2 echo "Cassandra is unavailable - sleeping";
  sleep 1;
done
```

The `storage.hostname` property is used when running tests to indicate an external instance should be used. Depending on the tests being run it may be necessary to provide the Docker container IP address rather than the host address (127.0.0.1) to avoid test failures. The Docker container IP address can be obtained as shown below.

```bash
STORAGE_HOSTNAME=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' jg-cassandra`
```

After running tests the container can be stopped and removed as shown below

```bash
docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml stop
docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml rm -f
```

#### Default Tests

Default Thrift tests with the `Murmur3Partitioner` partitioner:

```bash
CASSANDRA_VERSION=3.11.0 docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -pl janusgraph-cassandra -Dtest=**/thrift/* -Dtest.skip.ordered=true -Dtest.skip.ssl=true -Dstorage.hostname=$STORAGE_HOSTNAME
```

Default Thrift tests with the `ByteOrderedPartitioner` partitioner:

```bash
CASSANDRA_VERSION=3.11.0 CASSANDRA_ENABLE_BOP=true docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -pl janusgraph-cassandra -Dtest=**/thrift/* -Dtest.skip.unordered=true -Dtest.skip.ssl=true -Dtest.skip.serial=true -Dstorage.hostname=$STORAGE_HOSTNAME
```

Default Thrift SSL tests with the `Murmur3Partitioner` partitioner:

```bash
CASSANDRA_VERSION=3.11.0 CASSANDRA_ENABLE_SSL=true docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -pl janusgraph-cassandra -Dtest=**/thrift/* -Dtest.skip.unordered=true -Dtest.skip.ordered=true -Dtest.skip.serial=true -Dtest.skip.serial=true -Dstorage.hostname=$STORAGE_HOSTNAME
```

To run default Astyanax or CQL tests change the `test` property value in the above calls. Also note that the CQL module uses different property names to toggle the partitioner and enable SSL.

| Description | Property (Cassandra Module) | Property (CQL Module) |
| ---- | ---- | ---- |
| Skip Murmur3Partitioner tests | test.skip.unordered | test.skip.murmur |
| Skip ByteOrderedPartitioner tests | test.skip.ordered | test.skip.byteorderedpartitioner |
| Skip SSL (murmur) | test.skip.ssl=true | test.skip.murmur-ssl=true |

#### TinkerPop Tests

TinkerPop Thrift tests:

```bash
CASSANDRA_VERSION=3.11.0 CASSANDRA_ENABLE_BOP=true docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -Dtest.skip.tp=false -DskipTests=true -pl janusgraph-cassandra -fn -Dstorage.hostname=$STORAGE_HOSTNAME
```

#### Hadoop Tests

Hadoop tests with Cassandra 2:

```bash
CASSANDRA_VERSION=2.2.10 docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -pl :janusgraph-hadoop -DskipHBase -Dstorage.hostname=$STORAGE_HOSTNAME
```

Hadoop tests with Cassandra 3 (note that default Cassandra 2 tests must be skipped):

```bash
CASSANDRA_VERSION=3.11.0 docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -pl :janusgraph-hadoop -DskipHBase -DskipCassandra -DskipCassandra3=false -Dstorage.hostname=$STORAGE_HOSTNAME
```
