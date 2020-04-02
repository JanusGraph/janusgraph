# Installation

## Running JanusGraph inside a Docker container

For virtualization and easy access, JanusGraph provides a [Docker image](https://hub.docker.com/r/janusgraph/janusgraph).
Docker makes it easier to run servers and clients on a single machine without dealing with multiple installations.
For instructions on installing and using Docker, please refer to the [docker guide](https://docker.com/get-started).
Let's try running a simple JanusGraph instance in Docker:
```bash
$ docker run -it -p 8182:8182 janusgraph/janusgraph
```
We run the image interactively and request Docker to make the container's port `8182` available for us to see.
The server may need a few seconds to start up so be patient and wait for the corresponding log messages to appear.

??? note "Example log"
    ```
    SLF4J: Class path contains multiple SLF4J bindings.
    SLF4J: Found binding in [jar:file:/opt/janusgraph/lib/slf4j-log4j12-1.7.12.jar!/org/slf4j/impl/StaticLoggerBinder.class]
    SLF4J: Found binding in [jar:file:/opt/janusgraph/lib/logback-classic-1.1.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
    SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
    SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
    0    [main] INFO  com.jcabi.manifests.Manifests  - 110 attributes loaded from 283 stream(s) in 130ms, 110 saved, 3770 ignored: ["Agent-Class", "Ant-Version", "Archiver-Version", "Automatic-Module-Name", "Bnd-LastModified", "Boot-Class-Path", "Branch", "Build-Date", "Build-Host", "Build-Id", "Build-Java-Version", "Build-Jdk", "Build-Job", "Build-Number", "Build-Timestamp", "Build-Version", "Built-At", "Built-By", "Built-Date", "Built-OS", "Built-On", "Built-Status", "Bundle-ActivationPolicy", "Bundle-Activator", "Bundle-BuddyPolicy", "Bundle-Category", "Bundle-ClassPath", "Bundle-ContactAddress", "Bundle-Description", "Bundle-DocURL", "Bundle-License", "Bundle-ManifestVersion", "Bundle-Name", "Bundle-NativeCode", "Bundle-RequiredExecutionEnvironment", "Bundle-SymbolicName", "Bundle-Vendor", "Bundle-Version", "Can-Redefine-Classes", "Change", "Class-Path", "Created-By", "DSTAMP", "DynamicImport-Package", "Eclipse-BuddyPolicy", "Eclipse-ExtensibleAPI", "Embed-Dependency", "Embed-Transitive", "Export-Package", "Extension-Name", "Extension-name", "Fragment-Host", "Gradle-Version", "Gremlin-Lib-Paths", "Gremlin-Plugin-Dependencies", "Gremlin-Plugin-Paths", "Ignore-Package", "Implementation-Build", "Implementation-Build-Date", "Implementation-Title", "Implementation-URL", "Implementation-Vendor", "Implementation-Vendor-Id", "Implementation-Version", "Import-Package", "Include-Resource", "JCabi-Build", "JCabi-Date", "JCabi-Version", "Java-Vendor", "Java-Version", "Main-Class", "Manifest-Version", "Maven-Version", "Module-Email", "Module-Origin", "Module-Owner", "Module-Source", "Originally-Created-By", "Os-Arch", "Os-Name", "Os-Version", "Package", "Premain-Class", "Private-Package", "Provide-Capability", "Require-Bundle", "Require-Capability", "Scm-Connection", "Scm-Revision", "Scm-Url", "Specification-Title", "Specification-Vendor", "Specification-Version", "TODAY", "TSTAMP", "Time-Zone-Database-Version", "Tool", "X-Compile-Elasticsearch-Snapshot", "X-Compile-Elasticsearch-Version", "X-Compile-Lucene-Version", "X-Compile-Source-JDK", "X-Compile-Target-JDK", "hash", "implementation-version", "mode", "package", "service", "url", "version"]
    1    [main] INFO  org.apache.tinkerpop.gremlin.server.GremlinServer  - 3.4.1
             \,,,/
             (o o)
    -----oOOo-(3)-oOOo-----

    100  [main] INFO  org.apache.tinkerpop.gremlin.server.GremlinServer  - Configuring Gremlin Server from /etc/opt/janusgraph/gremlin-server.yaml
    ...
    ...
    3965 [gremlin-server-boss-1] INFO  org.apache.tinkerpop.gremlin.server.GremlinServer  - Gremlin Server configured with worker thread pool of 1, gremlin pool of 8 and boss thread pool of 1.
    3965 [gremlin-server-boss-1] INFO  org.apache.tinkerpop.gremlin.server.GremlinServer  - Channel started at port 8182.
    ```

We can now start a Gremlin Console on our local device and try to connect to the new server:
```bash
$ bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
gremlin> :remote connect tinkerpop.server conf/remote.yaml
==>Configured localhost/127.0.0.1:8182
```
Notice that the client side of this works exactly the same as before when running both the client and server locally without Docker.

Conveniently, it's also possible to run both the server and the client within separate Docker containers.
We therefore instantiate a container for the server:
```bash
$ docker run --name janusgraph-default janusgraph/janusgraph:latest
```
We can now instruct Docker to start a second container for the client and link it to the already running server.
```bash
$ docker run --rm --link janusgraph-default:janusgraph -e GREMLIN_REMOTE_HOSTS=janusgraph \
    -it janusgraph/janusgraph:latest ./bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
gremlin> :remote connect tinkerpop.server conf/remote.yaml
==>Configured janusgraph/172.17.0.2:8182
```
Notice how it's not necessary to bind any ports in order to make this example work.
For further reading, see the [JanusGraph Server](../basics/server.md) section as well as the [JanusGraph Docker documentation](https://github.com/JanusGraph/janusgraph-docker/blob/master/README.md).

## Local Installation

In order to run JanusGraph, Java 8 SE is required.
Make sure the `$JAVA_HOME` environment variable points to the correct location where either JRE or JDK is installed.
JanusGraph can be downloaded as a .zip archive from the [Releases](https://github.com/JanusGraph/janusgraph/releases) section of the project repository.

```bash
$ unzip janusgraph-{{ latest_version }}-hadoop2.zip
Archive:  janusgraph-{{ latest_version }}-hadoop2.zip
  creating: janusgraph-{{ latest_version }}-hadoop2/
...
```

Once you have unzipped the downloaded archive, you are ready to go.

### Running the Gremlin Console

The Gremlin Console is an interactive shell that gives you access to the data managed by JanusGraph.
You can reach it by running the `gremlin.sh` script which is located in the project's `bin` directory.

```bash
$ cd janusgraph-{{ latest_version }}-hadoop2
$ bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
09:12:24 INFO  org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph  - HADOOP_GREMLIN_LIBS is set to: /usr/local/janusgraph/lib
plugin activated: tinkerpop.hadoop
plugin activated: janusgraph.imports
gremlin>
```

The Gremlin Console interprets commands using [Apache Groovy](https://www.groovy-lang.org/), which is a superset of Java.
Gremlin-Groovy extends Groovy by providing a set of methods for basic and advanced graph traversal funcionality.
For a deeper dive into Gremlin language's features, please refer to our [introduction to Gremlin](../basics/gremlin.md).

### Running the Gremlin Server

In most real-world use cases, queries to a database will not be run from the exact same server the data is stored on.
Instead, there will be some sort of client-server hierarchy in which the server runs the database and handles requests while multiple clients create these requests and thereby read and write entries within the database independently of one another.
This behavior can also be achieved with JanusGraph.

In order to start a server on your local machine, simply run the `gremlin-server.sh` script instead of the `gremlin.sh` script.
You can optionally pass a configuration file as a parameter.
The default configuration is located at `conf/gremlin-server/gremlin-server.yaml`.

```bash
$ ./bin/gremlin-server.sh start
```
or
```bash
$ ./bin/gremlin-server.sh /conf/gremlin-server/[...].yaml
```

!!! info
    By default, JanusGraph will try to use a CQL-compatible storage backend.
    To use another backend, the following line within the earlier mentioned default configuration must be altered:
    ```yaml
    graphs: {
        graph: conf/gremlin-server/janusgraph-cql-es-server.properties
    }
    ```
    To get started, the non-persistent `inmemory` backend provided by `conf/janusgraph-inmemory.properties` is the easiest option to use.
    For further information about storage backends, visit the [corresponding section](../storage-backend/index.md) of the documentation.

    You are also encouraged to look into `janusgraph.sh`, which by defaults starts a more sophisticated server than `gremlin-server.sh`.
    Further documentation on server configuration can be found in the [JanusGraph Server](../basics/server.md) section.

A Gremlin server is now running on your local machine and waiting for clients to connect on the default port `8182`.
To instantiate a client -- as done before -- run the `gremlin.sh` script.
Again, a local Gremlin Console will show up.
This time, instead of using it locally, we will connect the Gremlin Console to a remote server and redirect all of it's queries to this server.
This is done by using the `:remote` command:
```bash
gremlin> :remote connect tinkerpop.server conf/remote.yaml
==>Configured localhost/127.0.0.1:8182
```
As you can probably tell from the log, the client and server are running on the same machine in this case.
When using a different setup, all you have to do is modify the parameters in the `conf/remote.yaml` file.

!!! warning
    The above command only establishes the connection to the server.
    It does not forward the following commands to the server by default!
    As a result, further commands will still be executed locally unless preceeded by `:>`.

    To forward every command to the remote server, use the `:remote console` command.
    Further documentation can be found in the [TinkerPop reference docs](https://tinkerpop.apache.org/docs/{{ tinkerpop_version }}/reference/#console-remote-console)
