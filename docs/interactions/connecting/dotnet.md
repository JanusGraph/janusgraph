# Connecting from .NET

Gremlin traversals can be constructed with Gremlin.Net just like in
Gremlin-Java or Gremlin-Groovy. Refer to [Gremlin Query Language](../../getting-started/gremlin.md) for an
introduction to Gremlin and pointers to further resources. The main
syntactical difference for Gremlin.Net is that it follows .NET naming
conventions, e.g., method names use PascalCase instead of camelCase.

## Getting Started with JanusGraph and Gremlin.Net

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
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

var client = new GremlinClient(new GremlinServer("localhost", 8182));
// The client should be disposed on shut down to release resources
// and to close open connections with client.Dispose()
var g = Traversal().WithRemote(new DriverRemoteConnection(client));
        // Reuse 'g' across the application
```

1.  Execute a simple traversal:
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

## JanusGraph.Net for JanusGraph Specific Types and Predicates

JanusGraph contains some types and [predicates](../search-predicates.md) that
are not part of Apache TinkerPop and are therefore also not supported by
Gremlin.Net.
[JanusGraph.Net](https://github.com/JanusGraph/janusgraph-dotnet) is a .NET
library that adds support for these types and predicates to Gremlin.Net.

After installing the library, a message serializer needs to be configured for
JanusGraph which can be done like this for GraphSON 3:

```cs
var client = new GremlinClient(new GremlinServer("localhost", 8182),
    new JanusGraphGraphSONMessageSerializer());
```

or like this for GraphBinary:

```cs
var client = new GremlinClient(new GremlinServer("localhost", 8182),
    new GraphBinaryMessageSerializer(JanusGraphTypeSerializerRegistry.Instance));
```

Refer to [the documentation of JanusGraph.Net](https://github.com/JanusGraph/janusgraph-dotnet#janusgraphnet)
for more information about the library, including its [compatibility with
different JanusGraph versions](https://github.com/JanusGraph/janusgraph-dotnet#version-compatibility)
and differences in support between the different [serialization formats](https://github.com/JanusGraph/janusgraph-dotnet#serialization-formats).
