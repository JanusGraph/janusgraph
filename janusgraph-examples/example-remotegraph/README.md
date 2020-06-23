# Remote Graph

## About Remote Graph

[Gremlin Server](https://tinkerpop.apache.org/docs/3.2.6/reference/#gremlin-server)
allows a JanusGraph instance to run on a standalone server. This enables
multiple clients to connect to the same JanusGraph instance. It also allows
clients to connect via non-JVM languages. By default, the Gremlin Server
communicates over WebSocket.

[`RemoteGraphApp.java`](src/main/java/org/janusgraph/example/RemoteGraphApp.java)
is an example Java program that connects to the Gremlin Server in two ways:
* [Gremlin Driver](https://tinkerpop.apache.org/docs/3.2.6/reference/#connecting-via-java)
client. Sumbit scripts as a `String` for evaluation on the server.
* [Remote Graph](https://tinkerpop.apache.org/docs/3.2.6/reference/#connecting-via-remotegraph)
uses the same client connection but enables you to compose queries as code.

## JanusGraph configuration

### JanusGraph Server configuration

* Select a specific storage backend to use, then copy its configuration into
the `$JANUSGRAPH_HOME/conf/gremlin-server` directory

* Update the `$JANUSGRAPH_HOME/conf/gremlin-server/gremlin-server.yaml` to
use the configuration file, for example:

    ```
    graphs: {
        graph: conf/gremlin-server/jgex-cql.properties
    }
    ```

* Note the default `$JANUSGRAPH_HOME/conf/gremlin-server/gremlin-server.yaml`
uses the script `scripts/empty-graph.groovy` to define the graph traversal source
`g` for the graph `g: graph.traversal()`

### JanusGraph pre-packaged distribution

Rather than installing Cassandra and Elasticsearch separately, the JanusGraph
[pre-packaged distribution](https://docs.janusgraph.org/basics/server/#using-the-pre-packaged-distribution)
is provided for convenience. The distribution starts a local Cassandra,
Elasticsearch, and Gremlin Server.

### Remote Graph configuration

* [`jgex-remote.properties`](conf/jgex-remote.properties) contains the remote
connection class, the Gremlin Driver configuration file location, and the graph
traversal source name. Refer to the [Remote Graph documentation](https://tinkerpop.apache.org/docs/3.2.6/reference/#connecting-via-remotegraph)
for additional details.

* [`remote-objects.yaml`](conf/remote-objects.yaml) contains server location
and serializer options. The default serializer is Gryo, and it is important
that the server is configured with the same serializer. Refer to the
[Gremlin Driver documentation](https://tinkerpop.apache.org/docs/3.2.6/reference/#_configuration)
for additional properties.

## Dependencies

The required Maven dependency for remote graph:

```
        <dependency>
            <groupId>org.apache.tinkerpop</groupId>
            <artifactId>gremlin-driver</artifactId>
            <version>${tinkerpop.version}</version>
        </dependency>
```

## Run the example

### Start the JanusGraph Server

Start the JanusGraph Server with the preferred storage configuration. The graph
instance will be created if it did not previously exist. Make sure to check
the server logs for errors.

```
$JANUSGRAPH_HOME/bin/janusgraph-server.sh $JANUSGRAPH_HOME/conf/gremlin-server/gremlin-server.yaml
```

### Run the example application

This command can be run from the `examples` or the project's directory.

```
mvn exec:java -pl :example-remotegraph
```

## Drop the graph

Make sure to stop the application and the Gremlin Server before dropping
the graph. Follow the directions for the specific storage backend.
