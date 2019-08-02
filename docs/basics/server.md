# JanusGraph Server
JanusGraph uses the [Gremlin Server](https://tinkerpop.apache.org/docs/{{tinkerpop_version}}/reference/#gremlin-server) 
engine as the server component to process and answer client queries. 
When packaged in JanusGraph, Gremlin Server is called JanusGraph Server.

JanusGraph Server must be started manually in order to use it.
JanusGraph Server provides a way to remotely execute Gremlin traversals
against one or more JanusGraph instances hosted within it. This section
will describe how to use the WebSocket configuration, as well as
describe how to configure JanusGraph Server to handle HTTP endpoint
interactions. For information about how to connect to a JanusGraph
Server from different languages refer to [Connecting to JanusGraph](../connecting/index.md).

## Getting Started

### Using the Pre-Packaged Distribution

The JanusGraph
[release](https://github.com/JanusGraph/janusgraph/releases) comes
pre-configured to run JanusGraph Server out of the box leveraging a
sample Cassandra and Elasticsearch configuration to allow users to get
started quickly with JanusGraph Server. This configuration defaults to
client applications that can connect to JanusGraph Server via WebSocket
with a custom subprotocol. There are a number of clients developed in
different languages to help support the subprotocol. The most familiar
client to use the WebSocket interface is the Gremlin Console. The
quick-start bundle is not intended to be representative of a production
installation, but does provide a way to perform development with
JanusGraph Server, run tests and see how the components are wired
together. To use this default configuration:

-   Download a copy of the current `janusgraph-$VERSION.zip` file from
    the [Releases
    page](https://github.com/JanusGraph/janusgraph/releases)

-   Unzip it and enter the `janusgraph-$VERSION` directory

-   Run `bin/janusgraph.sh start`. This step will start Gremlin Server
    with Cassandra/ES forked into a separate process. Note for security
    reasons Elasticsearch and therefore `janusgraph.sh` must be run
    under a non-root account.

<!-- -->
```bash
$ bin/janusgraph.sh start
Forking Cassandra...
Running `nodetool statusthrift`.. OK (returned exit status 0 and printed string "running").
Forking Elasticsearch...
Connecting to Elasticsearch (127.0.0.1:9300)... OK (connected to 127.0.0.1:9300).
Forking Gremlin-Server...
Connecting to Gremlin-Server (127.0.0.1:8182)... OK (connected to 127.0.0.1:8182).
Run gremlin.sh to connect.
```

#### Connecting to Gremlin Server

After running `janusgraph.sh`, Gremlin Server will be ready to listen
for WebSocket connections. The easiest way to test the connection is
with Gremlin Console.

Start Gremlin Console with bin/gremlin.sh and use the :remote and :> commands to issue Gremlin to Gremlin Server:

```bash
$  bin/gremlin.sh
         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
plugin activated: tinkerpop.server
plugin activated: tinkerpop.hadoop
plugin activated: tinkerpop.utilities
plugin activated: janusgraph.imports
plugin activated: tinkerpop.tinkergraph
gremlin> :remote connect tinkerpop.server conf/remote.yaml
==>Connected - localhost/127.0.0.1:8182
gremlin> :> graph.addVertex("name", "stephen")
==>v[256]
gremlin> :> g.V().values('name')
==>stephen
```

The `:remote` command tells the console to configure a remote connection
to Gremlin Server using the `conf/remote.yaml` file to connect. That
file points to a Gremlin Server instance running on `localhost`. The
`:>` is the "submit" command which sends the Gremlin on that line to the
currently active remote. By default remote conenctions are sessionless,
meaning that each line sent in the console is interpreted as a single
request. Multiple statements can be sent on a single line using a
semicolon as the delimiter. Alternately, you can establish a console
with a session by specifying
[session](https://tinkerpop.apache.org/docs/current/reference/#sessions)
when creating the connection. A [console
session](https://tinkerpop.apache.org/docs/current/reference/#console-sessions)
allows you to reuse variables across several lines of input.
```groovy
gremlin> :remote connect tinkerpop.server conf/remote.yaml
==>Configured localhost/127.0.0.1:8182
gremlin> graph
==>standardjanusgraph[cql:[127.0.0.1]]
gremlin> g
==>graphtraversalsource[standardjanusgraph[cql:[127.0.0.1]], standard]
gremlin> g.V()
gremlin> user = "Chris"
==>Chris
gremlin> graph.addVertex("name", user)
No such property: user for class: Script21
Type ':help' or ':h' for help.
Display stack trace? [yN]
gremlin> :remote connect tinkerpop.server conf/remote.yaml session
==>Configured localhost/127.0.0.1:8182-[9acf239e-a3ed-4301-b33f-55c911e04052]
gremlin> g.V()
gremlin> user = "Chris"
==>Chris
gremlin> user
==>Chris
gremlin> graph.addVertex("name", user)
==>v[4344]
gremlin> g.V().values('name')
==>Chris
```

## Cleaning up after the Pre-Packaged Distribution

If you want to start fresh and remove the database and logs you can use
the clean command with `janusgraph.sh`. The server should be stopped
before running the clean operation.
```bash
$ cd /Path/to/janusgraph/janusgraph-0.2.0-hadoop2/
$ ./bin/janusgraph.sh stop
Killing Gremlin-Server (pid 91505)...
Killing Elasticsearch (pid 91402)...
Killing Cassandra (pid 91219)...
$ ./bin/janusgraph.sh clean
Are you sure you want to delete all stored data and logs? [y/N] y
Deleted data in /Path/to/janusgraph/janusgraph-0.2.0-hadoop2/db
Deleted logs in /Path/to/janusgraph/janusgraph-0.2.0-hadoop2/log
```

## JanusGraph Server as a WebSocket Endpoint

The default configuration described in [Getting Started](#getting-started) 
is already a WebSocket configuration.
If you want to alter the default configuration to work with your own
Cassandra or HBase environment rather than use the quick start
environment, follow these steps:

**To Configure JanusGraph Server For WebSocket**

1.  Test a local connection to a JanusGraph database first. This step
    applies whether using the Gremlin Console to test the connection, or
    whether connecting from a program. Make appropriate changes in a
    properties file in the `./conf` directory for your environment. For
    example, edit `./conf/janusgraph-hbase.properties` and make sure the
    storage.backend, storage.hostname and storage.hbase.table parameters
    are specified correctly. For more information on configuring
    JanusGraph for various storage backends, see
    [Storage Backends](../storage-backend/index.md). Make sure the properties file contains the
    following line:
```conf
gremlin.graph=org.janusgraph.core.JanusGraphFactory
```

2.  Once a local configuration is tested and you have a working
    properties file, copy the properties file from the `./conf`
    directory to the `./conf/gremlin-server` directory.
```bash
cp conf/janusgraph-hbase.properties
conf/gremlin-server/socket-janusgraph-hbase-server.properties
```

3.  Copy `./conf/gremlin-server/gremlin-server.yaml` to a new file
    called `socket-gremlin-server.yaml`. Do this in case you need to
    refer to the original version of the file
```bash
cp conf/gremlin-server/gremlin-server.yaml
conf/gremlin-server/socket-gremlin-server.yaml
```

4.  Edit the `socket-gremlin-server.yaml` file and make the following
    updates:

    1.  If you are planning to connect to JanusGraph Server from
        something other than localhost, update the IP address for host:
```conf
host: 10.10.10.100
```

    2.  Update the graphs section to point to your new properties file
        so the JanusGraph Server can find and connect to your JanusGraph
        instance:
```yaml
graphs: { graph:
    conf/gremlin-server/socket-janusgraph-hbase-server.properties}
```

5.  Start the JanusGraph Server, specifying the yaml file you just
    configured:
```bash
bin/gremlin-server.sh ./conf/gremlin-server/socket-gremlin-server.yaml
```
 
6.  The JanusGraph Server should now be running in WebSocket mode and
    can be tested by following the instructions in [Connecting to Gremlin Server](#first-example-connecting-gremlin-server)

!!! Important
    Do not use `bin/janusgraph.sh`. That starts the default
    configuration, which starts a separate Cassandra/Elasticsearch
    environment.

## JanusGraph Server as a HTTP Endpoint

The default configuration described in [Getting Started](#getting-started) is a WebSocket configuration. If you
want to alter the default configuration in order to use JanusGraph
Server as an HTTP endpoint for your JanusGraph database, follow these
steps:

1.  Test a local connection to a JanusGraph database first. This step
    applies whether using the Gremlin Console to test the connection, or
    whether connecting from a program. Make appropriate changes in a
    properties file in the `./conf` directory for your environment. For
    example, edit `./conf/janusgraph-hbase.properties` and make sure the
    storage.backend, storage.hostname and storage.hbase.table parameters
    are specified correctly. For more information on configuring
    JanusGraph for various storage backends, see
    [Storage Backends](../storage-backend/index.md). Make sure the properties file contains the
    following line:
```conf
gremlin.graph=org.janusgraph.core.JanusGraphFactory
```

2.  Once a local configuration is tested and you have a working
    properties file, copy the properties file from the `./conf`
    directory to the `./conf/gremlin-server` directory.
```bash
cp conf/janusgraph-hbase.properties conf/gremlin-server/http-janusgraph-hbase-server.properties
```

3.  Copy `./conf/gremlin-server/gremlin-server.yaml` to a new file
    called `http-gremlin-server.yaml`. Do this in case you need to refer
    to the original version of the file
```bash
cp conf/gremlin-server/gremlin-server.yaml conf/gremlin-server/http-gremlin-server.yaml
```

4.  Edit the `http-gremlin-server.yaml` file and make the following
    updates:

    1.  If you are planning to connect to JanusGraph Server from
        something other than localhost, update the IP address for host:
```conf
host: 10.10.10.100
```

    2.  Update the channelizer setting to specify the HttpChannelizer:
```yaml
channelizer: org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
```

    3.  Update the graphs section to point to your new properties file
        so the JanusGraph Server can find and connect to your JanusGraph
        instance:
```yaml
graphs: { graph:
    conf/gremlin-server/http-janusgraph-hbase-server.properties}
```

5.  Start the JanusGraph Server, specifying the yaml file you just
    configured:
```bash
bin/gremlin-server.sh ./conf/gremlin-server/http-gremlin-server.yaml
```

6.  The JanusGraph Server should now be running in HTTP mode and
    available for testing. **curl** can be used to verify the server is
    working:
```bash
curl -XPOST -Hcontent-type:application/json -d *{"gremlin":"g.V().count()"}* [IP for JanusGraph server host](http://):8182 
```

## JanusGraph Server as Both a WebSocket and HTTP Endpoint

As of JanusGraph 0.2.0, you can configure your `gremlin-server.yaml` to
accept both WebSocket and HTTP connections over the same port. This can
be achieved by changing the channelizer in any of the previous examples
as follows.
```yaml
channelizer: org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer
```
## Advanced JanusGraph Server Configurations
### Authentication over HTTP

!!! IMPORTANT
    In the following example, credentialsDb should be different from the graph(s) you are using. It should be configured with the correct backend and a different keyspace, table, or storage directory as appropriate for the configured backend. This graph will be used for storing usernames and passwords.

### HTTP Basic authentication

To enable Basic authentication in JanusGraph Server include the following configuration in your `gremlin-server.yaml`.

```yaml
 authentication: {
   authenticator: org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.JanusGraphSimpleAuthenticator,
   authenticationHandler: org.apache.tinkerpop.gremlin.server.handler.HttpBasicAuthenticationHandler,
   config: {
     defaultUsername: user,
     defaultPassword: password,
     credentialsDb: conf/janusgraph-credentials-server.properties
    }
 }
```

Verify that basic authentication is configured correctly. For example

```bash
curl -v -XPOST http://localhost:8182 -d '{"gremlin": "g.V().count()"}'
```

should return a 401 if the authentication is configured correctly and

```bash
curl -v -XPOST http://localhost:8182 -d '{"gremlin": "g.V().count()"}' -u user:password
```
should return a 200 and the result of 4 if authentication is configured correctly.

### Authentication over WebSocket

Authentication over WebSocket occurs through a Simple Authentication and Security Layer (https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer[SASL]) mechanism.


To enable SASL authentication include the following configuration in the `gremlin-server.yaml`

```yaml
authentication: {
  authenticator: org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.JanusGraphSimpleAuthenticator,
  authenticationHandler: org.apache.tinkerpop.gremlin.server.handler.SaslAuthenticationHandler,
  config: {
    defaultUsername: user,
    defaultPassword: password,
    credentialsDb: conf/janusgraph-credentials-server.properties
  }
}
```

!!! important
    In the preceding example, credentialsDb should be different from the graph(s) you are using. It should be configured with the correct backend and a different keyspace, table, or storage directory as appropriate for the configured backend. This graph will be used for storing usernames and passwords.

If you are connecting through the gremlin console, your remote yaml file should ammend the `username` and `password` properties with the appropriate values.

```yaml
username: user
password: password
```

### Authentication over HTTP and WebSocket

If you are using the combined channelizer for both HTTP and WebSocket you can use the SaslAndHMACAuthenticator to authorize through either WebSocket through SASL, HTTP through basic auth, and HTTP through hash-based messsage authentication code (https://en.wikipedia.org/wiki/Hash-based_message_authentication_code[HMAC]) Auth. HMAC is a token based authentication designed to be used over HTTP. You first acquire a token via the `/session` endpoint and then use that to authenticate. It is used to amortize the time spent encrypting the password using basic auth.

The `gremlin-server.yaml` should include the following configurations

```yaml
authentication: {
  authenticator: org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.SaslAndHMACAuthenticator,
  authenticationHandler: org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.SaslAndHMACAuthenticationHandler,
  config: {
    defaultUsername: user,
    defaultPassword: password,
    hmacSecret: secret,
    credentialsDb: conf/janusgraph-credentials-server.properties
  }
}
```

!!! important
    In the preceding example, credentialsDb should be different from the graph(s) you are using. It should be configured with the correct backend and a different keyspace, table, or storage directory as appropriate for the configured backend. This graph will be used for storing usernames and passwords.

!!! important
    Note the hmacSecret here. This should be the same across all running JanusGraph servers if you want to be able to use the same HMAC token on each server.

For HMAC authentication over HTTP, this creates a `/session` endpoint that provides a token that expires after an hour by default. This timeout for the token can be configured through the `tokenTimeout` configuration option in the `authentication.config` map. This value is a Long value and in milliseconds.

You can obtain the token using curl by issuing a get request to the `/session` endpoint. For example

```bash
curl http://localhost:8182/session -XGET -u user:password

{"token": "dXNlcjoxNTA5NTQ2NjI0NDUzOkhrclhYaGhRVG9KTnVSRXJ5U2VpdndhalJRcVBtWEpSMzh5WldqRTM4MW89"}
```

You can then use that token for authentication by using the "Authorization: Token" header. For example

```bash
curl -v http://localhost:8182/session -XPOST -d '{"gremlin": "g.V().count()"}' -H "Authorization: Token dXNlcjoxNTA5NTQ2NjI0NDUzOkhrclhYaGhRVG9KTnVSRXJ5U2VpdndhalJRcVBtWEpSMzh5WldqRTM4MW89"
```

### Using TinkerPop Gremlin Server with JanusGraph

Since JanusGraph Server is a TinkerPop Gremlin Server packaged 
with configuration files for JanusGraph, a version compatible 
TinkerPop Gremlin Server can be downloaded separately and used with JanusGraph. 
Get started by downloading the appropriate version of Gremlin Server, 
which needs to match a version supported by the JanusGraph version in use ({{ tinkerpop_version }}).

!!! important
    Any references to file paths in this section refer to paths under a
    TinkerPop distribution for Gremlin Server and not a JanusGraph
    distribution with the JanusGraph Server, unless specifically noted.

Configuring a standalone Gremlin Server to work with 
JanusGraph is similar to configuring the packaged JanusGraph Server. 
You should be familiar with [graph configuration](https://tinkerpop.apache.org/docs/{{ tinkerpop_version }}/reference/#_configuring_2). 
Basically, the Gremlin Server yaml file points to graph-specific configuration files 
that are used to instantiate JanusGraph instances that it will then host. 
In order to instantiate these Graph instances, 
Gremlin Server requires that the appropriate libraries and dependencies 
for the JanusGraph be available on its classpath.

For purposes of demonstration, these instructions will outline how to
configure the BerkeleyDB backend for JanusGraph in Gremlin Server. As
stated earlier, Gremlin Server needs JanusGraph dependencies on its
classpath. Invoke the following command replacing `$VERSION` with the
version of JanusGraph to use:

```bash
bin/gremlin-server.sh -i org.janusgraph janusgraph-all $VERSION
```

When this process completes, Gremlin Server should now have all the
JanusGraph dependencies available to it and will thus be able to
instantiate `JanusGraph` objects.

!!! important
    The above command uses Groovy Grape and if it is not configured properly 
    download errors may ensue. Please refer to [this section](https://tinkerpop.apache.org/docs/{{tinkerpop_version}} /reference/#gremlin-applications) 
    of the TinkerPop documentation for more information around setting up ~/.groovy/grapeConfig.xml.

Create a file called `GREMLIN_SERVER_HOME/conf/janusgraph.properties`
with the following contents:
```conf
gremlin.graph=org.janusgraph.core.JanusGraphFactory
storage.backend=berkeleyje
storage.directory=db/berkeley
```

Configuration of other backends is similar. See
[Storage Backends](../storage-backend/index.md). If using Cassandra, then use Cassandra
configuration options in the `janusgraph.properties` file. The only
important piece to leave unchanged is the `gremlin.graph` setting which
should always use `JanusGraphFactory`. This setting tells Gremlin Server
how to instantiate a `JanusGraph` instance.

Next create a file called
`GREMLIN_SERVER_HOME/conf/gremlin-server-janusgraph.yaml` that has the
following contents:
```yaml
host: localhost
port: 8182
graphs: {
  graph: conf/janusgraph.properties}
scriptEngines: {
  gremlin-groovy: {
    plugins: { org.janusgraph.graphdb.tinkerpop.plugin.JanusGraphGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.server.jsr223.GremlinServerGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin: {classImports: [java.lang.Math], methodImports: [java.lang.Math#*]},
               org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin: {files: [scripts/empty-sample.groovy]}}}}
serializers:
  - { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0, config: { ioRegistries: [org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] }}
  - { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0, config: { serializeResultToString: true }}
  - { className: org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0, config: { ioRegistries: [org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] }}
metrics: {
  slf4jReporter: {enabled: true, interval: 180000}}
```

**There are several important parts to this configuration file as they relate to JanusGraph.**

1.  In the `graphs` map, there is a key called `graph` and its value is
    `conf/janusgraph.properties`. This tells Gremlin Server to
    instantiate a `Graph` instance called "graph" and use the
    `conf/janusgraph.properties` file to configure it. The "graph" key
    becomes the unique name for the `Graph` instance in Gremlin Server
    and it can be referenced as such in the scripts submitted to it.

2.  In the `plugins` list, there is a reference to
    `JanusGraphGremlinPlugin`, which tells Gremlin Server to initialize
    the "JanusGraph Plugin". The "JanusGraph Plugin" will auto-import
    JanusGraph specific classes for usage in scripts.

3.  Note the `scripts` key and the reference to
    `scripts/janusgraph.groovy`. This Groovy file is an initialization
    script for Gremlin Server and that particular ScriptEngine. Create
    `scripts/janusgraph.groovy` with the following contents:

```groovy
def globals = [:]
globals << [g : graph.traversal()]
```

The above script creates a `Map` called `globals` and assigns to it a
key/value pair. The key is `g` and its value is a `TraversalSource`
generated from `graph`, which was configured for Gremlin Server in its
configuration file. At this point, there are now two global variables
available to scripts provided to Gremlin Server - `graph` and `g`.

At this point, Gremlin Server is configured and can be used to connect
to a new or existing JanusGraph database. To start the server:
```bash
$ bin/gremlin-server.sh conf/gremlin-server-janusgraph.yaml
[INFO] GremlinServer -
         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----

[INFO] GremlinServer - Configuring Gremlin Server from conf/gremlin-server-janusgraph.yaml
[INFO] MetricManager - Configured Metrics Slf4jReporter configured with interval=180000ms and loggerName=org.apache.tinkerpop.gremlin.server.Settings$Slf4jReporterMetrics
[INFO] GraphDatabaseConfiguration - Set default timestamp provider MICRO
[INFO] GraphDatabaseConfiguration - Generated unique-instance-id=7f0000016240-ubuntu1
[INFO] Backend - Initiated backend operations thread pool of size 8
[INFO] KCVSLog$MessagePuller - Loaded unidentified ReadMarker start time 2015-10-02T12:28:24.411Z into org.janusgraph.diskstorage.log.kcvs.KCVSLog$MessagePuller@35399441
[INFO] GraphManager - Graph [graph] was successfully configured via [conf/janusgraph.properties].
[INFO] ServerGremlinExecutor - Initialized Gremlin thread pool.  Threads in pool named with pattern gremlin-*
[INFO] ScriptEngines - Loaded gremlin-groovy ScriptEngine
[INFO] GremlinExecutor - Initialized gremlin-groovy ScriptEngine with scripts/janusgraph.groovy
[INFO] ServerGremlinExecutor - Initialized GremlinExecutor and configured ScriptEngines.
[INFO] ServerGremlinExecutor - A GraphTraversalSource is now bound to [g] with graphtraversalsource[standardjanusgraph[berkeleyje:db/berkeley], standard]
[INFO] AbstractChannelizer - Configured application/vnd.gremlin-v3.0+gryo with org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0
[INFO] AbstractChannelizer - Configured application/vnd.gremlin-v3.0+gryo-stringd with org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0
[INFO] GremlinServer$1 - Gremlin Server configured with worker thread pool of 1, gremlin pool of 8 and boss thread pool of 1.
[INFO] GremlinServer$1 - Channel started at port 8182.
```

The following section explains how to connect to the running server.

#### Connecting to JanusGraph via Gremlin Server

Gremlin Server will be ready to listen for WebSocket connections when it
is started. The easiest way to test the connection is with Gremlin
Console.

Follow the instructions here [Connecting to Gremlin Server](../connecting/index.md) to verify the Gremlin
Server is working.

!!! important
    A difference you should understand is that when working with
    JanusGraph Server, the Gremlin Console is started from underneath the
    JanusGraph distribution and when following the test instructions here
    for a standalone Gremlin Server, the Gremlin Console is started from
    under the TinkerPop distribution.

```java
GryoMapper mapper = GryoMapper.build().addRegistry(JanusGraphIoRegistry.INSTANCE).create();
Cluster cluster = Cluster.build().serializer(new GryoMessageSerializerV3d0(mapper)).create();
Client client = cluster.connect();
client.submit("g.V()").all().get();
```

By adding the `JanusGraphIoRegistry` to the
`org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0`, the
driver will know how to properly deserialize custom data types returned
by JanusGraph.

## Extending JanusGraph Server

It is possible to extend Gremlin Server with other means of
communication by implementing the interfaces that it provides and
leverage this with JanusGraph. See more details in the appropriate
TinkerPop documentation.
