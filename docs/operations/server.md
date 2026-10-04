# JanusGraph Server
JanusGraph uses the [Gremlin Server](https://tinkerpop.apache.org/docs/{{tinkerpop_version}}/reference/#gremlin-server) 
engine as the server component to process and answer client queries and extends it with convenience features for JanusGraph. 
From now on, we will call this JanusGraph Server.

JanusGraph Server must be started manually in order to use it.
JanusGraph Server provides a way to remotely execute Gremlin traversals
against one or more JanusGraph instances hosted within it. This section
will describe how to use the WebSocket configuration, as well as
describe how to configure JanusGraph Server to handle HTTP endpoint
interactions. For information about how to connect to a JanusGraph
Server from different languages refer to [Connecting to JanusGraph](../basics/connecting/index.md).

## Starting a JanusGraph Server

JanusGraph Server comes packaged with a script called `bin/janusgraph-server.sh` to get it started:
```txt
$ ./bin/janusgraph-server.sh console
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/var/lib/janusgraph/lib/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/var/lib/janusgraph/lib/logback-classic-1.1.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
0    [main] INFO  org.janusgraph.graphdb.server.JanusGraphServer  -                                                                       
   mmm                                mmm                       #     
     #   mmm   m mm   m   m   mmm   m"   "  m mm   mmm   mmmm   # mm  
     #  "   #  #"  #  #   #  #   "  #   mm  #"  " "   #  #" "#  #"  # 
     #  m"""#  #   #  #   #   """m  #    #  #     m"""#  #   #  #   # 
 "mmm"  "mm"#  #   #  "mm"#  "mmm"   "mmm"  #     "mm"#  ##m#"  #   # 
                                                         #            
                                                         "            
[...]
2240 [gremlin-server-boss-1] INFO  org.apache.tinkerpop.gremlin.server.GremlinServer  - Channel started at port 8182.
```


JanusGraph Server is configured by the provided YAML file `conf/gremlin-server/gremlin-server.yaml`. 
That file tells JanusGraph Server many things and is based on the Gremlin Server config, see [Gremlin Server](https://tinkerpop.apache.org/docs/current/reference/#gremlin-server).

### Usage of janusgraph-server.sh

The JanusGraph Server can be started in the foreground with stdout logging or detached. 

```txt
$ ./bin/janusgraph-server.sh
Usage: ./bin/janusgraph-server.sh {start [conf file]|stop|restart [conf file]|status|console|usage <group> <artifact> <version>|<conf file>}

    start           Start the server in the background. Configuration file can be specified as a second argument
                    or as JANUSGRAPH_YAML environment variable. If configuration file is not specified
                    or has invalid path than JanusGraph server will try to use the default configuration file
                    at relative location conf/gremlin-server/gremlin-server.yaml
    stop            Stop the server
    restart         Stop and start the server. To use previously used configuration it should be specified again
                    as described in "start" command
    status          Check if the server is running
    console         Start the server in the foreground. Same rules are applied for configurations as described
                    in "start" command
    usage           Print out this help message

In case command is not specified and the configuration is specified as the first argument, JanusGraph Server will
 be started in the foreground using the specified configuration (same as with "console" command).
```

### Env variables

| Variable | Description | Default Value |
|---|---|---|
|`$JANUSGRAPH_HOME`| Root directory of a default janusgraph installation. | (default directory below janusgraph-server.sh) |
|`$JANUSGRAPH_CONF`| Config directory containing all kinds of server and graph configs. | `$JANUSGRAPH_HOME/conf` |
|`$LOG_DIR`| Log directory |`"$JANUSGRAPH_HOME/logs"`|
|`$LOG_FILE`| Default log file |`"$LOG_DIR/janusgraph.log"`|
|`$PID_DIR`||`"$JANUSGRAPH_HOME/run"`|
|`$PID_FILE`||`"$PID_DIR/janusgraph.pid"`|
|`$JANUSGRAPH_YAML`| JanusGraph Server config path |`"$JANUSGRAPH_CONF/gremlin-server/gremlin-server.yaml"`|
|`$JANUSGRAPH_LIB`| JanusGraph library directory |`"$JANUSGRAPH_HOME/lib"`|
|`$JAVA_HOME`| If not set java home fallback to `java`. |NOT_SET|
|`$JAVA_OPTIONS_FILE`||`"$JANUSGRAPH_CONF/jvm.options"`|
|`$JAVA_OPTIONS`||NOT_SET|
|`$CP`| Can be used to override the classpath's. (expert mode) |NOT_SET|
|`$DEBUG`| If you enable debug by creating this env, bash debug will be enabled. |NOT_SET|

### Configure jvm.options

JanusGraph runs on the JVM which is configurable for special use cases. Therefore, JanusGraph provides a `jvm.options` file with some default options.

```bash
# Copyright 2020 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#################
# HEAP SETTINGS #
#################

-Xms4096m
-Xmx4096m


########################
# GENERAL JVM SETTINGS #
########################


# enable thread priorities, primarily so we can give periodic tasks
# a lower priority to avoid interfering with client workload
-XX:+UseThreadPriorities

# allows lowering thread priority without being root on linux - probably
# not necessary on Windows but doesn't harm anything.
# see http://tech.stolsvik.com/2010/01/linux-java-thread-priorities-workar
-XX:ThreadPriorityPolicy=42

# Enable heap-dump if there's an OOM
-XX:+HeapDumpOnOutOfMemoryError

# Per-thread stack size.
-Xss256k

# Make sure all memory is faulted and zeroed on startup.
# This helps prevent soft faults in containers and makes
# transparent hugepage allocation more effective.
-XX:+AlwaysPreTouch

# Enable thread-local allocation blocks and allow the JVM to automatically
# resize them at runtime.
-XX:+UseTLAB
-XX:+ResizeTLAB
-XX:+UseNUMA


####################
# GREMLIN SETTINGS #
####################

-Dgremlin.io.kryoShimService=org.janusgraph.hadoop.serialize.JanusGraphKryoShimService


#################
#  GC SETTINGS  #
#################

### CMS Settings

-XX:+UseParNewGC
-XX:+UseConcMarkSweepGC
-XX:+CMSParallelRemarkEnabled
-XX:SurvivorRatio=8
-XX:MaxTenuringThreshold=1
-XX:CMSInitiatingOccupancyFraction=75
-XX:+UseCMSInitiatingOccupancyOnly
-XX:CMSWaitDuration=10000
-XX:+CMSParallelInitialMarkEnabled
-XX:+CMSEdenChunksRecordAlways
-XX:+CMSClassUnloadingEnabled
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
```properties
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
bin/janusgraph-server.sh console ./conf/gremlin-server/socket-gremlin-server.yaml
```
 
6.  The JanusGraph Server should now be running in WebSocket mode and
    can be tested by following the instructions in [Connecting to Gremlin Server](../getting-started/installation.md)

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
```properties
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
bin/janusgraph-server.sh console ./conf/gremlin-server/http-gremlin-server.yaml
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

If you are connecting through the gremlin console, your remote yaml file should amend the `username` and `password` properties with the appropriate values.

```yaml
username: user
password: password
```

### Authentication over HTTP and WebSocket

If you are using the combined channelizer for both HTTP and WebSocket you can use the SaslAndHMACAuthenticator to authorize through either WebSocket through SASL, HTTP through basic auth, and HTTP through hash-based message authentication code (https://en.wikipedia.org/wiki/Hash-based_message_authentication_code[HMAC]) Auth. HMAC is a token based authentication designed to be used over HTTP. You first acquire a token via the `/session` endpoint and then use that to authenticate. It is used to amortize the time spent encrypting the password using basic auth.

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

### Gremlin pool on virtual threads

JanusGraph Server evaluates requests in the Gremlin pool of Gremlin Server. The pool has `gremlinPool` threads, by
default as many as the JVM has processors, and a queue for `maxWorkQueueSize` more requests, 8192 by default. A request
which finds the queue full is rejected: with the status `TOO_MANY_REQUESTS` over WebSocket, with the status 500 over
HTTP. Requests in a session are evaluated on a thread of their session instead, unless the deprecated
`UnifiedChannelizer` is configured.

A thread of the pool stays with its request while the request waits for the storage and index backends, and a request
whose data isn't cached spends most of its time waiting. The pool therefore evaluates only as many requests per second
as `gremlinPool` threads can wait for, however idle the processors are, and raising `gremlinPool` creates as many
platform threads.

On Java 24 or later, in practice Java 25, the long-term-support release, as Java 24 is out of support, the setting
`gremlinPoolVirtualThreads` runs the pool on virtual threads:

```yaml
gremlinPool: 256
gremlinPoolVirtualThreads: true
```

The pool keeps its bounds: `gremlinPool` requests are evaluated at once, `maxWorkQueueSize` more wait, and any further
request is rejected as before. Its threads are named `gremlin-server-exec-<n>`, like those of Gremlin Server's pool. A
virtual thread whose request waits for a backend releases the platform thread it ran on, so `gremlinPool` can be raised
to the number of requests which should be evaluated at once without creating as many platform threads. A thread which
has had no request to evaluate for a minute ends. The setting is off by default. With it on, JanusGraph Server does not
start on Java versions older than 24: the future which `JanusGraphServer.start()` returns fails with an
`IllegalStateException` which names the Java version it runs on, and `JanusGraphServer.main` logs the error and stops.

Java 21 has virtual threads as well, but before Java 24 a virtual thread which waits inside a `synchronized` method or
block keeps its platform thread, its carrier, so at most as many such waits proceed at once as there are carriers, by
default one per processor. JanusGraph waits inside `synchronized` methods while a transaction commits and while it waits
for a new block of IDs, and with `cache.db-cache` a cache miss reads the storage backend inside a `synchronized` block.
On Java 21, a pool of 256 virtual threads committed only about half as many write transactions per second as a pool
of 256 platform threads, 12,500 against 22,200 at 256 in flight, while the server used under two processors (measured
as below, with the same pool built outside JanusGraph Server, which refuses the setting on Java 21).

The following was measured on one machine (Apple M5 Pro, 18 processors) with Cassandra 5.0.8 in a container, a graph
of 20,000 vertices with 10 edges each and a composite index, and JanusGraph Server on Java 25 with a 2 GB heap and the
default graph configuration, so without `cache.db-cache`. Gremlin drivers kept a fixed number of requests in flight; a
request is a sessionless bytecode traversal, so a transaction of its own: 40% read a vertex by id, 30% its neighbours,
10% counted the neighbours of the neighbours, and 20% looked a vertex up by the composite index. Each cell is the mean
of two runs of 15 s after 20 s of warm-up.

| Gremlin pool | 16 in flight | 256 in flight | 1024 in flight | Platform threads of the server |
|---|---|---|---|---|
| 18 platform threads (the default) | 4,430 requests/s | 5,480 requests/s | 5,500 requests/s | 78–88 |
| 256 platform threads | 4,520 requests/s | 16,800 requests/s | 18,900 requests/s | 326 |
| 256 virtual threads | 4,420 requests/s | 20,900 requests/s | 20,700 requests/s | 90 |
| 1024 platform threads | 3,590 requests/s | 10,200 requests/s | 3,870 requests/s | 1,094 |
| 1024 virtual threads | 4,780 requests/s | 21,500 requests/s | 21,100 requests/s | 90 |

The default pool capped the throughput at about 5,500 requests per second while the server used about one processor.
With 256 threads, virtual threads answered a quarter more requests per second than platform threads at 256 in flight
and took a third less processor time per request (200 instead of 306 µs). A pool of 1,024 platform threads was slower
than one of 256 at every level and collapsed at 1,024 requests in flight, with a p99 latency of 530 ms and 1,500 µs of
processor time per request, while 1,024 virtual threads kept the throughput of 256 on 90 platform threads. At 16
requests in flight, where the pool isn't the bottleneck, the pools of 18 and 256 threads and the 1,024 virtual threads
answered between 4,400 and 4,800 requests per second, the 1,024 platform threads 3,590.

A mix of vertex and edge insertions, half of each, behaved alike: at 256 in flight, 256 virtual threads committed
27,700 transactions per second against 22,800 with 256 platform threads, at 152 instead of 231 µs of processor time
per transaction, with 115 platform threads in the server instead of 351.

Before enabling the setting, consider the following:

* Raise `gremlinPool` along with it. The setting doesn't change how many requests are evaluated at once, only what they
  run on, so with the default `gremlinPool` it changes little.
* The backends keep their own limits. The CQL backend, for example, has at most `storage.cql.back-pressure-limit`
  requests in flight, by default 1024 for each Cassandra node, and further requests wait. A `gremlinPool` far above
  what the backends serve at once only adds waiting requests.
* Requests in a session are still evaluated on a platform thread of their session.
* Java 24 lets a virtual thread release its carrier while it waits inside `synchronized`, but a virtual thread still
  keeps its carrier while it runs native code. The virtual threads run on `jdk.virtualThreadScheduler.parallelism`
  carriers, by default as many as the JVM has processors, and `jdk.virtualThreadScheduler.maxPoolSize`, by default 256
  or the parallelism if that is larger, bounds the carriers the scheduler may add temporarily.
* Code which caches per thread in a `ThreadLocal` keeps one copy for each of the `gremlinPool` threads, that is one per
  request evaluated at once, and loses a copy when its thread ends after a minute without requests.
* `jstack` and `jcmd <pid> Thread.print` list platform threads only. The threads of the pool don't appear in them; a
  carrier which is running one of them is marked `Carrying virtual thread #<n>` and shows its own frames, not those of
  the request. `jcmd <pid> Thread.dump_to_file <file>`, with or without `-format=json`, lists every thread of the pool
  by name with its stack.
* The official JanusGraph container images run on Java 11. The setting needs an image built from a base image with
  Java 24 or later, which the `BASE_IMAGE` build argument of the Dockerfile selects, and the settings passed through the
  environment of the container, `gremlinserver.gremlinPoolVirtualThreads=true` and `gremlinserver.gremlinPool=256`, as
  the image sets `gremlinPool` to 8.

## Extending JanusGraph Server

!!! note
    We currently are refactoring JanusGraph Server. If you like to get information or want to give input, see [issue #2119](https://github.com/JanusGraph/janusgraph/issues/2119).

It is possible to extend Gremlin Server with other means of
communication by implementing the interfaces that it provides and
leverage this with JanusGraph. See more details in the appropriate
TinkerPop documentation.
