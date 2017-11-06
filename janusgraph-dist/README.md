janusgraph-dist
==========

Building zip archives
-----------------------------

Run `mvn clean install -Pjanusgraph-release -Dgpg.skip=true
-DskipTests=true`.  This command can be run from either the root of
the JanusGraph repository (the parent of the janusgraph-dist directory) or the
janusgraph-dist directory.  Running from the root of the repository is
recommended.  Running from janusgraph-dist requries that JanusGraph's jars be
available on either Sonatype, Maven Central, or your local Maven
repository (~/.m2/repository/) depending on whether you're building a
SNAPSHOT or a release tag.

This command writes one archive:

* janusgraph-dist/janusgraph-dist-hadoop-2/target/janusgraph-$VERSION-hadoop2.zip

It's also possible to leave off the `-DskipTests=true`.  However, in
the absence of `-DskipTests=true`, the -Pjanusgraph-release argument
causes janusgraph-dist to run several automated integration tests of the
zipfiles and the script files they contain.  These tests require unzip
and expect, and they'll start and stop Cassandra, ES, and HBase in the
course of their execution.

Building documentation
----------------------

To convert the AsciiDoc sources in $JANUSGRAPH_REPO_ROOT/docs/ to chunked
and single-page HTML, run `mvn package -pl janusgraph-dist -am
-DskipTests=true -Dgpg.skip=true` from the directory containing
janusgraph-dist.  If the JanusGraph artifacts are already installed in the local
Maven repo from previous invocations of `mvn install` in the root of
the clone, then `cd janusgraph-dist && mvn package` is also sufficient.

The documentation output appears in:

* janusgraph-dist/target/docs/chunk/
* janusgraph-dist/target/docs/single/

Building deb/rpm packages
-------------------------

Requires:

* a platform that can run shell scripts (e.g. Linux, Mac OS X, or
  Windows with Cygwin)

* the Aurelius public package GPG signing key

Run `mvn -N -Ppkg-tools install` in the janusgraph-dist module.  This writes
three folders to the root of the janusgraph repository:

* debian
* pkgcommon
* redhat

The debian and redhat folders contain platform-specific packaging
conttrol and payoad files.  The pkgcommon folder contains shared
payload and helper scripts.

To build the .deb and .rpm packages:

* (cd to the repository root)
* `pkgcommon/bin/build-all.sh`

To delete the packaging scripts from the root of the repository, run
`mvn -N -Ppkg-tools clean` from the janusgraph-dist module.

Building the Docker Image
-------------------------

Requirements

* [Docker](https://www.docker.com/)

First, build the zip archive from the previous section. Next run the command
`mvn dockerfile:build -Pjanusgraph-docker -pl janusgraph-dist` from the root directory of this 
repository. This command will build a JanusGraph docker image tagged with the current project 
version. If you want to specify a different tag, you can run the command as
`mvn dockerfile:build -Ddocker.tag=latest -Pjanusgraph-docker -pl janusgraph-dist`.

Running the Docker Image
------------------------

By default, the docker image is built using JanusGraph's [in memory](http://docs.janusgraph.org/latest/inmemorystorage.html)
storage engine. You can run an instance of JanusGraph with the command
`docker run -p8182:8182 experoinc/janusgraph:latest`. It also supports integration with other 
storage and index backends. The simplest way to integrate with different engines is to use 
[Docker Compose](https://docs.docker.com/compose/). Below is an example compose file that integrates
with [Apache Cassandra](http://cassandra.apache.org/) and [ElasticSearch](https://www.elastic.co/).

```yaml
# docker-compose.yml
version: '2.1'

services:
  janusgraph:
    image: janusgraph/janusgraph:janusgraph
    container_name: jce-janusgraph
    environment:
      JANUS_STORAGE_BACKEND: cassandra
      JANUS_STORAGE_HOSTNAME: cassandra-host
      JANUS_INDEX_DEFINED_NAME: search
      JANUS_INDEX_HOSTNAME: elasticsearch
    ports:
      - "8182:8182"
    links:
      - cassandra:cassandra-host
      - elasticsearch:elasticsearch
    depends_on:
      cassandra:
        condition: service_healthy
    healthcheck:
        test: [ "CMD-SHELL", "timeout -t 2 bash -c 'cat < /dev/null > /dev/tcp/127.0.0.1/8182'" ]
        interval: 10s
        timeout: 10s
        retries: 6

  cassandra:
    image: cassandra:3
    container_name: jce-cassandra
    ports:
      - "9042:9042"
      - "9160:9160"
    environment:
      CASSANDRA_START_RPC: 'true'
    healthcheck:
      test: [ "CMD-SHELL", "timeout 2 bash -c 'cat < /dev/null > /dev/tcp/127.0.0.1/9160'" ]
      interval: 10s
      timeout: 10s
      retries: 6

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:5.3.2
    container_name: jce-elastic
    environment:
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - "http.host=0.0.0.0"
      - "network.host=0.0.0.0"
      - "transport.host=127.0.0.1"
      - "cluster.name=docker-cluster"
      - "xpack.security.enabled=false"
      - "discovery.zen.minimum_master_nodes=1"
    ports:
      - "9200:9200"
    healthcheck:
        test: [ "CMD-SHELL", "timeout -t 5 bash -c 'curl --output /dev/null --silent --head --fail http://127.0.0.1:9200'" ]
        interval: 10s
        timeout: 10s
        retries: 6
```

You can bring up this configuration by running `docker-compose up` from the directory where the 
`docker-compose.yml` file resides.

The JanusGraph image is configured through environment variables. Here is a list of configurable
environment variables and their corresponding [configuration key](http://docs.janusgraph.org/latest/config-ref.html)
of the JanusGraph properties file or [Gremlin Server](http://tinkerpop.apache.org/docs/3.0.1-incubating/#_configuring_2).

### gremlin-server.yaml ###

| VARIABLE                                  | CONFIG KEY                                         | DEFAULT          |
|-------------------------------------------|----------------------------------------------------|------------------|
| JANUS_BIND_HOST                           | host                                               | 0.0.0.0          |
| JANUS_BIND_PORT                           | port                                               | 8182             |
| JANUS_WORKER_POOL_SIZE                    | threadPoolWorker                                   | 1                |
| JANUS_GREMLIN_POOL_SIZE                   | gremlinPool                                        | 8                |
|                                           |                                                    |                  |

### janusgraph.properties ###


| VARIABLE                                  | CONFIG KEY                                         | DEFAULT          |
|-------------------------------------------|----------------------------------------------------|------------------|
| JANUS_STORAGE_BACKEND                     | storage.backend                                    | inmemory         |
| JANUS_STORAGE_BATCH_LOADING               | storage.batch-loading                              | FALSE            |
| JANUS_STORAGE_BUFFER_SIZE                 | storage.buffer-size                                | 1024             |
| JANUS_STORAGE_CONNECTION_TIMEOUT          | storage.connection-timeout                         | 10000            |
| JANUS_STORAGE_DIRECTORY                   | storage.directory                                  |                  |
| JANUS_STORAGE_HOSTNAME                    | storage.hostname                                   |                  |
| JANUS_STORAGE_PAGE_SIZE                   | storage.page-size                                  | 100              |
| JANUS_STORAGE_PARALLEL_BACKEND_OPS        | storage.parallel-backend-ops                       | TRUE             |
| JANUS_STORAGE_PASSWORD                    | storage.password                                   |                  |
| JANUS_STORAGE_PORT                        | storage.port                                       |                  |
| JANUS_STORAGE_READ_ONLY                   | storage.read-only                                  | FALSE            |
| JANUS_STORAGE_READ_TIME                   | storage.read-time                                  | 10000            |
| JANUS_STORAGE_SETUP_WAIT                  | storage.setup-wait                                 | 60000            |
| JANUS_STORAGE_TRANSACTIONS                | storage.transactions                               | TRUE             |
| JANUS_STORAGE_USERNAME                    | storage.username                                   |                  |
| JANUS_STORAGE_WRITE_TIME                  | storage.write-time                                 | 100000           |
|                                           |                                                    |                  |
| JANUS_INDEX_DEFINED_NAME                  | The (X) of index configurations                    |                  |
| JANUS_INDEX_BACKEND                       | index.(X).backend                                  | elasticsearch    |
| JANUS_INDEX_HOSTNAME                      | index.(X).hostname                                 | elstic-host      |
| JANUS_INDEX_NAME                          | index.(X).index-name                               | janusgraph       |
| JANUS_INDEX_MAP_NAME                      | index.(X).map-name                                 | TRUE             |
| JANUS_INDEX_MAX_RESULT_SET                | index.(X).max-result-set-size                      | 100000           |
| JANUS_INDEX_PORT                          | index.(X).port                                     |                  |
|                                           |                                                    |                  |
| JANUS_INDEX_ES_CLIENT_ONLY                | index.(X).elasticsearch.client-only                | TRUE             |
| JANUS_INDEX_ES_CLUSTER_NAME               | index.(X).elasticsearch.cluster-name               | elasticsearch    |
| JANUS_INDEX_ES_HEALTH_TIMEOUT             | index.(X).elasticsearch.health-request-timeout     | 30s              |
| JANUS_INDEX_ES_IGNORE_CLUSTER_NAME        | index.(X).elasticsearch.ignore-cluster-name        | TRUE             |
| JANUS_INDEX_ES_INTERFACE                  | index.(X).elasticsearch.interface                  | TRANSPORT_CLIENT |
| JANUS_INDEX_ES_LOAD_DEFAULT_NODE_SETTINGS | index.(X).elasticsearch.load-default-node-settings | TRUE             |
| JANUS_INDEX_ES_LOCAL_MODE                 | index.(X).elasticsearch.local-mode                 | FALSE            |
| JANUS_INDEX_ES_SNIFF                      | index.(X).elasticsearch.sniff                      | TRUE             |
| JANUS_INDEX_ES_TTL_INTERVAL               | index.(X).elasticsearch.ttl-interval               | 5s               |
| JANUS_INDEX_ES_CREATE_SLEEP               | index.(X).elasticsearch.create.sleep               | 200              |

Publishing the Docker Image
---------------------------

Requirements

* A [Docker Hub](https://hub.docker.com/) account
* Write access to the JanusGraph organization

You'll need to update your `~/.m2/settings.xml` to include your Docker Hub credentials for the 
`docker.io` server.

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
    https://maven.apache.org/xsd/settings-1.0.0.xsd">
    <servers>
        <server>
            <id>docker.io</id>
            <username>my-docker-hub-user</username>
            <password>my-docker-hub-password</password>
        </server>
    </servers>
</settings>
```

Then run the command `mvn dockerfile:push -Pjanusgraph-docker -pl janusgraph-dist` to push the 
image tagged with the current version of the repository. To push a different tag, use the docker.tag
property like so `mvn dockerfile:push -Ddocker.tag=latest -Pjanusgraph-docker -pl janusgraph-dist`.

Gollum-site is no longer required
---------------------------------

Previous versions of janusgraph-dist needed a companion module called
janusgraph-site, which in turn required the gollum-site binary to be
command on the local system.  This is no longer required now that the
docs have moved from the GitHub wiki to AsciiDoc files stored in the
repo.  The AsciiDoc files are converted to HTML using a DocBook-based
toolchain completely managed by maven.
