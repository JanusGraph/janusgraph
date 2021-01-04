# Configuration

A JanusGraph graph database cluster consists of one or multiple
JanusGraph instances. To open a JanusGraph instance, a configuration has
to be provided which specifies how JanusGraph should be set up.

A JanusGraph configuration specifies which components JanusGraph should
use, controls all operational aspects of a JanusGraph deployment, and
provides a number of tuning options to get maximum performance from a
JanusGraph cluster.

At a minimum, a JanusGraph configuration must define the persistence
engine that JanusGraph should use as a storage backend.
[Storage Backends](../storage-backend/index.md) lists all supported persistence engines and how
to configure them respectively. If advanced graph query support (e.g
full-text search, geo search, or range queries) is required an
additional indexing backend must be configured. See
[Index Backends](../index-backend/index.md) for details. If query performance is a concern,
then caching should be enabled. Cache configuration and tuning is
described in [JanusGraph Cache](#caching).

## Example Configurations

Below are some example configuration files to demonstrate how to
configure the most commonly used storage backends, indexing systems, and
performance components. This covers only a tiny portion of the available
configuration options. Refer to [Configuration Reference](configuration-reference.md)
for the complete list of all options.

### Cassandra+Elasticsearch

Sets up JanusGraph to use the Cassandra persistence engine running
locally and a remote Elastic search indexing system:

```properties
storage.backend=cql
storage.hostname=localhost

index.search.backend=elasticsearch
index.search.hostname=100.100.101.1, 100.100.101.2
index.search.elasticsearch.client-only=true
```

### HBase+Caching

Sets up JanusGraph to use the HBase persistence engine running remotely
and uses JanusGraph’s caching component for better performance.
```properties
storage.backend=hbase
storage.hostname=100.100.101.1
storage.port=2181

cache.db-cache = true
cache.db-cache-clean-wait = 20
cache.db-cache-time = 180000
cache.db-cache-size = 0.5
```

### BerkeleyDB

Sets up JanusGraph to use BerkeleyDB as an embedded persistence engine
with Elasticsearch as an embedded indexing system.
```properties
storage.backend=berkeleyje
storage.directory=/tmp/graph

index.search.backend=elasticsearch
index.search.directory=/tmp/searchindex
index.search.elasticsearch.client-only=false
index.search.elasticsearch.local-mode=true
```

[Configuration Reference](configuration-reference.md) describes all of these
configuration options in detail. The `conf` directory of the JanusGraph
distribution contains additional configuration examples.

### Further Examples

There are several example configuration files in the `conf/` directory
that can be used to get started with JanusGraph quickly. Paths to these
files can be passed to `JanusGraphFactory.open(...)` as shown below:

```groovy
// Connect to Cassandra on localhost using a default configuration
graph = JanusGraphFactory.open("conf/janusgraph-cql.properties")
// Connect to HBase on localhost using a default configuration
graph = JanusGraphFactory.open("conf/janusgraph-hbase.properties")
```

## Using Configuration

How the configuration is provided to JanusGraph depends on the
instantiation mode.

### JanusGraphFactory

#### Gremlin Console

The JanusGraph distribution contains a command line Gremlin Console
which makes it easy to get started and interact with JanusGraph. Invoke
`bin/gremlin.sh` (Unix/Linux) or `bin/gremlin.bat` (Windows) to start
the Console and then open a JanusGraph graph using the factory with the
configuration stored in an accessible properties configuration file:
```groovy
graph = JanusGraphFactory.open('path/to/configuration.properties')
```

#### JanusGraph Embedded

JanusGraphFactory can also be used to open an embedded JanusGraph graph
instance from within a JVM-based user application. In that case,
JanusGraph is part of the user application and the application can call
upon JanusGraph directly through its public API.

#### Short Codes

If the JanusGraph graph cluster has been previously configured and/or
only the storage backend needs to be defined, JanusGraphFactory accepts
a colon-separated string representation of the storage backend name and
hostname or directory.
```groovy
graph = JanusGraphFactory.open('cql:localhost')
graph = JanusGraphFactory.open('berkeleyje:/tmp/graph')
```

### JanusGraph Server

JanusGraph, by itself, is simply a set of jar files with no thread of
execution. There are two basic patterns for connecting to, and using a
JanusGraph database:

1.  JanusGraph can be used by embedding JanusGraph calls in a client
    program where the program provides the thread of execution.

2.  JanusGraph packages a long running server process that, when
    started, allows a remote client or logic running in a separate
    program to make JanusGraph calls. This long running server process
    is called **JanusGraph Server**.
  
For the JanusGraph Server, JanusGraph uses [Gremlin Server](https://tinkerpop.apache.org/docs/{{ tinkerpop_version }}/reference/#gremlin-server) of the [Apache TinkerPop](https://tinkerpop.apache.org/) stack to service client requests. JanusGraph provides an out-of-the-box configuration for a quick start with JanusGraph Server, but the configuration can be changed to provide a wide range of server capabilities.

Configuring JanusGraph Server is accomplished through a JanusGraph
Server yaml configuration file located in the ./conf/gremlin-server
directory in the JanusGraph distribution. To configure JanusGraph Server
with a graph instance (`JanusGraph`), the JanusGraph Server
configuration file requires the following settings:

```yaml
...
graphs: {
  graph: conf/janusgraph-berkeleyje.properties
}
scriptEngines: {
  gremlin-groovy: {
    plugins: { org.janusgraph.graphdb.tinkerpop.plugin.JanusGraphGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.server.jsr223.GremlinServerGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin: {classImports: [java.lang.Math], methodImports: [java.lang.Math#*]},
               org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin: {files: [scripts/empty-sample.groovy]}}}}
...
```

The entry for `graphs` defines the bindings to specific `JanusGraph`
configurations. In the above case it binds `graph` to a JanusGraph
configuration at `conf/janusgraph-berkeleyje.properties`. The `plugins`
entry enables the JanusGraph Gremlin Plugin, which enables auto-imports
of JanusGraph classes so that they can be referenced in remotely
submitted scripts.

Learn more about configuring and using JanusGraph Server in [JanusGraph Server](../operations/server.md).

#### Server Distribution

The JanusGraph zip file contains a quick start server component that
helps make it easier to get started with Gremlin Server and JanusGraph.
Invoke `bin/janusgraph.sh start` to start Gremlin Server with Cassandra
and Elasticsearch.

!!! note
    For security reasons Elasticsearch and therefore `janusgraph.sh` must
    be run under a non-root account

!!! note
    Starting with 0.5.1, this is just included in the full package version.

## Global Configuration

JanusGraph distinguishes between local and global configuration options.
Local configuration options apply to an individual JanusGraph instance.
Global configuration options apply to all instances in a cluster. More
specifically, JanusGraph distinguishes the following five scopes for
configuration options:

-   **LOCAL**: These options only apply to an individual JanusGraph
    instance and are specified in the configuration provided when
    initializing the JanusGraph instance.

-   **MASKABLE**: These configuration options can be overwritten for an
    individual JanusGraph instance by the local configuration file. If
    the local configuration file does not specify the option, its value
    is read from the global JanusGraph cluster configuration.

-   **GLOBAL**: These options are always read from the cluster
    configuration and cannot be overwritten on an instance basis.

-   **GLOBAL\_OFFLINE**: Like *GLOBAL*, but changing these options
    requires a cluster restart to ensure that the value is the same
    across the entire cluster.

-   **FIXED**: Like *GLOBAL*, but the value cannot be changed once the
    JanusGraph cluster is initialized.

When the first JanusGraph instance in a cluster is started, the global
configuration options are initialized from the provided local
configuration file. Subsequently changing global configuration options
is done through JanusGraph’s management API. To access the management
API, call `g.getManagementSystem()` on an open JanusGraph instance
handle `g`. For example, to change the default caching behavior on a
JanusGraph cluster:
```groovy
mgmt = graph.openManagement()
mgmt.get('cache.db-cache')
// Prints the current config setting
mgmt.set('cache.db-cache', true)
// Changes option
mgmt.get('cache.db-cache')
// Prints 'true'
mgmt.commit()
// Changes take effect
```

### Changing Offline Options

Changing configuration options does not affect running instances and
only applies to newly started ones. Changing *GLOBAL\_OFFLINE*
configuration options requires restarting the cluster so that the
changes take effect immediately for all instances. To change
*GLOBAL\_OFFLINE* options follow these steps:

- Close all but one JanusGraph instance in the cluster
- Connect to the single instance
- Ensure all running transactions are closed
- Ensure no new transactions are started (i.e. the cluster must be offline)
- Open the management API
- Change the configuration option(s)
- Call commit which will automatically shut down the graph instance
- Restart all instances

Refer to the full list of configuration options in [Configuration Reference](configuration-reference.md) for more information including the configuration
scope of each option.