JanusGraph can be queried from all languages for which a TinkerPop
driver exists. Drivers allow sending of Gremlin traversals to a Gremlin
Server like the [JanusGraph Server](basics/server.md). A list of TinkerPop
drivers is available on [TinkerPop’s
homepage](http://tinkerpop.apache.org/#language-drivers).

In addition to drivers, there exist [query languages for
TinkerPop](http://tinkerpop.apache.org/#language-variants-compilers)
that make it easier to use Gremlin in different programming languages
like Java, Python, or C\#. Some of these languages even construct
Gremlin traversals from completely different query languages like Cypher
or SPARQL. Since JanusGraph implements TinkerPop, all of these languages
can be used together with JanusGraph.

## Connecting from Java


While it is possible to embed JanusGraph as a library inside a Java
application and then directly connect to the backend, this section
assumes that the application connects to JanusGraph Server. For
information on how to embed JanusGraph, see the [JanusGraph Examples
projects](https://github.com/JanusGraph/janusgraph/tree/master/janusgraph-examples).

This section only covers how applications can connect to JanusGraph
Server. Refer to [Gremlin Query Language](basics/gremlin.md) for an introduction to Gremlin and
pointers to further resources.

### Getting Started with JanusGraph and Gremlin-Java

To get started with JanusGraph in Java:

1.  Create an application with Maven:
```bash
mvn archetype:generate -DgroupId=com.mycompany.project
    -DartifactId=gremlin-example
    -DarchetypeArtifactId=maven-archetype-quickstart
    -DinteractiveMode=false
```
2.  Add dependencies on `janusgraph-core` and `gremlin-driver` to the dependency manager:

```xml tab='Maven'
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-core</artifactId>
    <version>{{ latest_version }}</version>
</dependency>
<dependency>
    <groupId>org.apache.tinkerpop</groupId>
    <artifactId>gremlin-driver</artifactId>
    <version>{{ tinkerpop_version }}</version>
</dependency>
```

```groovy tab='Gradle'
compile "org.janusgraph:janusgraph-core:{{ latest_version }}"
compile "org.apache.tinkerpop:gremlin-driver:{{ tinkerpop_version }}"
```

3.  Add two configuration files, `conf/remote-graph.properties` and
    `conf/remote-objects.yaml`:

```conf tab='conf/remote-graph.properties'
gremlin.remote.remoteConnectionClass=org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
gremlin.remote.driver.clusterFile=conf/remote-objects.yaml
gremlin.remote.driver.sourceName=g
```

```yaml tab='conf/remote-objects.yaml'
hosts: [localhost]
port: 8182
serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0,
              config: { ioRegistries: [org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] }}
```

4.  Create a `GraphTraversalSource` which is the basis for all Gremlin traversals:
```java
    Graph graph = EmptyGraph.instance();
    GraphTraversalSource g = graph.traversal().withRemote("conf/remote-graph.properties");
    // Reuse 'g' across the application
    // and close it on shut-down to close open connections with g.close()
```
5.  Execute a simple traversal:
```java
Object herculesAge = g.V().has("name", "hercules").values("age").next();
System.out.println("Hercules is " + herculesAge + " years old.");
```
`next()` is a terminal step that submits the traversal to the Gremlin Server and returns a single result.

### JanusGraph Specific Types and Predicates

JanusGraph specific types and [predicates](index-backend/search-predicates.md) can be
used directly from a Java application through the dependency
`janusgraph-core`.

## Connecting from Python

Gremlin traversals can be constructed with Gremlin-Python just like in
Gremlin-Java or Gremiln-Groovy. Refer to [Gremlin Query Language](basics/gremlin.md) for an
introduction to Gremlin and pointers to further resources.

!!! important
    Some Gremlin step and predicate names are reserved words in Python.
    Those names are simply postfixed with `_` in Gremlin-Python, e.g.,
    `in()` becomes `in_()`, `not()` becomes `not_()`, and so on. The other
    names affected by this are: `all`, `and`, `as`, `from`, `global`,
    `is`, `list`, `or`, and `set`.

### Getting Started with JanusGraph and Gremlin-Python

To get started with Gremlin-Python:

1.  Install Gremlin-Python:
```bash
pip install gremlinpython=={{ tinkerpop_version }}
```
1.  Create a text file `gremlinexample.py` and add the following imports
    to it:
```python
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
```

2.  Create a `GraphTraversalSource` which is the basis for all Gremlin
    traversals:
```python
graph = Graph()
connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
// The connection should be closed on shut down to close open connections with connection.close()
g = graph.traversal().withRemote(connection)
// Reuse 'g' across the application
```

3.  Execute a simple traversal:
```python
herculesAge = g.V().has('name', 'hercules').values('age').next()
print('Hercules is {} years old.'.format(herculesAge))
```
    `next()` is a terminal step that submits the traversal to the
    Gremlin Server and returns a single result.

### JanusGraph Specific Types and Predicates

JanusGraph contains some types and [predicates](index-backend/search-predicates.md) that
are not part of Apache TinkerPop and are therefore also not supported by
Gremlin-Python.

## Connecting from .NET

Gremlin traversals can be constructed with Gremlin.Net just like in
Gremlin-Java or Gremiln-Groovy. Refer to [Gremlin Query Language](basics/gremlin.md) for an
introduction to Gremlin and pointers to further resources. The main
syntactical difference for Gremlin.Net is that it follows .NET naming
conventions, e.g., method names use PascalCase instead of camelCase.

### Getting Started with JanusGraph and Gremlin.Net

To get started with Gremlin.Net:

1.  Create a console application:
```bash
dotnet new console -o GremlinExample
```

1.  Add Gremlin.Net:
```bash
dotnet add package Gremlin.Net -v {{ tinkerpop_version }}
```

1.  Create a `GraphTraversalSource` which is the basis for all Gremlin
    traversals:
```csharp
var graph = new Graph();
var client = new GremlinClient(new GremlinServer("localhost", 8182));
// The client should be disposed on shut down to release resources
// and to close open connections with client.Dispose()
var g = graph.Traversal().WithRemote(new DriverRemoteConnection(client));
        // Reuse 'g' across the application
```

2.  Execute a simple traversal:
```csharp
var herculesAge = g.V().Has("name", "hercules").Values<int>("age").Next();
Console.WriteLine($"Hercules is {herculesAge} years old.");
```
    The traversal can also be executed asynchronously by using
    `Promise()` which is the recommended way as the underlying driver in
    Gremlin.Net also works asynchronously:
```csharp
var herculesAge = await g.V().Has("name", "hercules").Values<int>("age").Promise(t => t.Next());
```
### JanusGraph Specific Types and Predicates

JanusGraph contains some types and [predicates](index-backend/search-predicates.md) that
are not part of Apache TinkerPop and are therefore also not supported by
Gremlin.Net.
