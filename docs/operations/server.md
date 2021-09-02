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
Server from different languages refer to [Connecting to JanusGraph](../interactions/connecting/index.md).

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

## Extending JanusGraph Server

!!! note
    We currently are refactoring JanusGraph Server. If you like to get information or want to give input, see [issue #2119](https://github.com/JanusGraph/janusgraph/issues/2119).

It is possible to extend Gremlin Server with other means of
communication by implementing the interfaces that it provides and
leverage this with JanusGraph. See more details in the appropriate
TinkerPop documentation.
