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

### JUnit Test Categories

All of JanusGraph's tests are written for JUnit.  JanusGraph's JUnit tests are annotated with the following [JUnit Categories](https://github.com/junit-team/junit/wiki/Categories):


| Category Name | Maven Property | Default | Comment |
| ------------- | ------------------- |:------------:| ------- |
| MemoryTests | test.skip.mem | true (disabled) | Tests intended to exert memory pressure |
| PerformanceTests | test.skip.perf | true (disabled) | Tests written as simple speed tests using JUnitBenchmarks|
| OrderedKeyStoreTests | test.skip.ordered | false (enabled) | Tests written for a storage backend that stores data in key order |
| UnorderedKeyStoreTests | test.skip.unordered | false (enabled) | Tests written for a storage backend that doesn't store data in key order |
| (No&nbsp;category) | test.skip.default | false (enabled) | Tests without any Category annotations |

**Category Name** above is a Java interface defined in the package [org.janusgraph.testcategory](janusgraph-test/src/main/org/janusgraph/testcategory).  These interfaces appear as arguments to the JUnit `@Category(...)` annotation, e.g. `@Category({MemoryTests.class})`.

**Maven Property** above is a boolean-valued pom.xml property that skips the associated test category when true and executes the associated test category when false.  The default values defined in pom.xml can be overridden on the command-line in the ordinary Maven way, e.g. `mvn -Dtest.skip.mem=false test`.

*Implementation Note.*  The Maven property naming pattern "test.skip.x=boolean" is needlessly verbose, a cardinal sin for command line options.  A more concise alternative would be "test.x" with the boolean sense negated.  However, this complicates the pom.xml configuration for the Surefire plugin since it precludes direct use of the Surefire plugin's `<skip>` configuration tag, as in `<skip>${test.skip.perf}</skip>`.  There doesn't seem to be a straightforward way to negate a boolean or otherwise make this easy, at least without resorting to profiles or a custom plugin, though I might be missing something.  Also, the mold is arguably already set by Surefire's "maven.test.skip" property, though that has slightly different interpretation semantics than the properties above.

### Running a Single Test via Maven

The standard maven-surefire-plugin option applies for most tests:

```bash
mvn test -Dtest=full.or.partial.classname#methodname
```

However, MemoryTests and PerformanceTests are disabled by default regardless of whatever `-Dtest=...` option might be specified.  When running a single MemoryTest or PerformanceTest, specify `-Dtest.mem=true` or `-Dtest.perf=true` as appropriate for the test in question.

Here's a concrete example.

```bash
# Executes no tests because the MemoryTests category is disabled by default
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

```bash
mvn clean install -pl janusgraph-solr -Pdocker,solr5
```

Finally the `solr.test.version` property can be used to test against arbitrary Solr versions.

```bash
mvn clean install -pl janusgraph-solr -Pdocker -Dsolr.test.version=5.5.4
```

### Running Tests with an External Elasticsearch

Elasticsearch tests can be run against an external Elasticsearch instance. For convenience the `es-docker` Maven profile is provided to manage an Elasticsearch Docker container through the Maven Failsafe Plugin. The default test version will be the same as the Elasticsearch client version.

```bash
mvn clean install -pl janusgraph-es -Pes-docker
```

Additional Maven profiles are defined for testing against default versions of other supported major Elasticsearch releases.

```bash
mvn clean install -pl janusgraph-es -Pes-docker,elasticsearch5
```

Finally the `elasticsearch.docker.test.version` property can be used to test against arbitrary Elasticsearch versions. This is more complicated however because of differences across major versions in required server settings, Docker image names and zipfile artifact availability. The examples below illustrate the differences based on the Elasticsearch major version.

```bash
mvn clean install -pl janusgraph-es -Pes-docker -Delasticsearch.docker.test.version=5.3.2
mvn clean install -pl janusgraph-es -Pes-docker -Delasticsearch.test.version=2.3.3 -Delasticsearch.test.groovy.inline="script.engine.groovy.inline.update: true" -Des.docker.image=elasticsearch
mvn clean install -pl janusgraph-es -Pes-docker -Delasticsearch.docker.test.version=1.5.1 -Delasticsearch.test.version=2.4.4 -Delasticsearch.test.groovy.inline="script.disable_dynamic: false" -Des.docker.image=elasticsearch
```

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

TinkerPop Thrift and CQL tests:

```bash
CASSANDRA_VERSION=3.11.0 CASSANDRA_ENABLE_BOP=true docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -Dtest.skip.tp=false -DskipTests=true -pl janusgraph-cassandra,janusgraph-cql -fn -Dstorage.hostname=$STORAGE_HOSTNAME
```

#### Hadoop Tests

Hadoop tests with Cassandra 2:

```bash
CASSANDRA_VERSION=2.2.10 docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -pl :janusgraph-hadoop-2 -DskipHBase -Dstorage.hostname=$STORAGE_HOSTNAME
```

Hadoop tests with Cassandra 3 (note that default Cassandra 2 tests must be skipped):

```bash
CASSANDRA_VERSION=3.11.0 docker-compose -f janusgraph-cassandra/src/test/resources/docker-compose.yml up -d
# wait for instance to start (see above)
mvn clean install -pl :janusgraph-hadoop-2 -DskipHBase -DskipCassandra -DskipCassandra3=false -Dstorage.hostname=$STORAGE_HOSTNAME
```

### Running Tests with ScyllaDB

Thrift and CQL tests can be run against an externally-managed [ScyllaDB](http://www.scylladb.com/) instance. For convenience the `scylladb-test` Maven profile is provided to manage a ScyllaDB Docker container through the Maven Failsafe Plugin. Note this only runs tests with the `Murmur3Partitioner` partitioner and also skips SSL tests.

```bash
mvn clean install -pl janusgraph-cql -Pscylladb-test
```
