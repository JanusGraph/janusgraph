# Remote Graph

## About Remote Graph

[Gremlin Server](http://tinkerpop.apache.org/docs/3.2.6/reference/#gremlin-server)
allows a JanusGraph instance to run on a standalone server. This enables
multiple clients to connect to the same JanusGraph instance. It also allows
clients to connect via non-JVM languages. By default, the Gremlin Server
communicates over WebSocket.

A Java program can connect to the Gremlin Server in two ways:
* [Gremlin Driver](http://tinkerpop.apache.org/docs/3.2.6/reference/#connecting-via-java)
client. Sumbit scripts as a `String` for evaluation on the server.
* [Remote Graph](http://tinkerpop.apache.org/docs/3.2.6/reference/#connecting-via-remotegraph)
uses the same client connection but enables you to compose queries as code.

## JanusGraph configuration

### Gremlin Server configuration

* Select a specific storage backend to use, then copy its configuration into
the `$JANUSGRAPH_HOME/conf/gremlin-server/` directory

* Update the `$JANUSGRAPH_HOME/conf/gremlin-server/gremlin-server.yaml` to
use the configuration file

    ```
    graphs: {
        graph: conf/gremlin-server/jgex-cql.properties
    }
    ```

* Note the default `$JANUSGRAPH_HOME/conf/gremlin-server/gremlin-server.yaml`
uses the script `scripts/empty-graph.groovy` to define the graph traversal source
`g` for the graph `g: graph.traversal()`

### Remote Graph configuration

* [`jgex-remote.properties`](conf/jgex-remote.properties) contains the remote
connection class, the Gremlin Driver configuration file location, and the graph
traversal source name. Refer to the [Remote Graph documentation](http://tinkerpop.apache.org/docs/3.2.6/reference/#connecting-via-remotegraph)
for additional details.

* [`remote-objects.yaml`](conf/remote-objects.yaml) contains server location
and serializer options. The default serializer is Gryo, and it is important
that the server is configured with the same serializer. Refer to the
[Gremlin Driver documentation](http://tinkerpop.apache.org/docs/3.2.6/reference/#_configuration)
for additional properties.

## Running the example

### Starting the Gremlin Server

Start the Gremlin Server with the preferred storage configuration. The graph
instance will be created if it did not previously exist. Make sure to check
the server logs for errors.

```
$ cd $JANUSGRAPH_HOME

$ bin/gremlin-server.sh conf/gremlin-server/gremlin-server.yaml
```

### Running the remote graph example

```
$ cd $JANUSGRAPH_HOME/janusgraph-examples/example-remotegraph

$ mvn exec:java -Dexec.mainClass="org.janusgraph.example.RemoteGraphApp" -Dexec.args="conf/jgex-remote.properties"
```

## Drop the graph

Make sure to stop the Gremlin Server before dropping the graph. Follow the
directions for the specific storage backend.
