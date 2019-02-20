Gremlin Query Language
======================

[Gremlin](http://tinkerpop.apache.org/gremlin.html) is JanusGraph’s
query language used to retrieve data from and modify data in the graph.
Gremlin is a path-oriented language which succinctly expresses complex
graph traversals and mutation operations. Gremlin is a [functional
language](http://en.wikipedia.org/wiki/Functional_programming) whereby
traversal operators are chained together to form path-like expressions.
For example, "from Hercules, traverse to his father and then his
father’s father and return the grandfather’s name."

Gremlin is a component of [Apache
TinkerPop](http://tinkerpop.apache.org). It is developed independently
from JanusGraph and is supported by most graph databases. By building
applications on top of JanusGraph through the Gremlin query language,
users avoid vendor-lock in because their application can be migrated to
other graph databases supporting Gremlin.

This section is a brief overview of the Gremlin query language. For more
information on Gremlin, refer to the following resources:

- [Practical Gremlin Book](https://github.com/krlawrence/graph#practical-gremlin-an-apache-tinkerpop-tutorial):
    A getting started guide for users of graph databases and the Gremlin
    query language.

- [Gremlin Language Drivers](http://tinkerpop.apache.org/index.html#language-drivers):
    Connect to a Gremlin Server with different programming languages,
    including Go, JavaScript, .NET/C\#, PHP, Python, Ruby, Scala, and
    TypeScript.

- [Gremlin for SQL developers](http://sql2gremlin.com): Learn Gremlin
    using typical patterns found when querying data with SQL.

In addition to these resources, [Connecting to JanusGraph](../connecting/index.md) explains how Gremlin
can be used in different programming languages to query a JanusGraph
Server.

Introductory Traversals
-----------------------

A Gremlin query is a chain of operations/functions that are evaluated
from left to right. A simple grandfather query is provided below over
the *Graph of the Gods* dataset discussed in [Getting Started](../intro.md#getting-started).
```groovy
gremlin> g.V().has('name', 'hercules').out('father').out('father').values('name')
==>saturn
```

The query above can be read:

1.  `g`: for the current graph traversal.
2.  `V`: for all vertices in the graph
3.  `has('name', 'hercules')`: filters the vertices down to those with name property "hercules" (there is only one).
4.  `out('father')`: traverse outgoing father edge’s from Hercules.
5.  ‘out('father')\`: traverse outgoing father edge’s from Hercules’ father’s vertex (i.e. Jupiter).
6.  `name`: get the name property of the "hercules" vertex’s grandfather.

Taken together, these steps form a path-like traversal query. Each step
can be decomposed and its results demonstrated. This style of building
up a traversal/query is useful when constructing larger, complex query
chains.

```groovy
gremlin> g
==>graphtraversalsource[janusgraph[cql:127.0.0.1], standard]
gremlin> g.V().has('name', 'hercules')
==>v[24]
gremlin> g.V().has('name', 'hercules').out('father')
==>v[16]
gremlin> g.V().has('name', 'hercules').out('father').out('father')
==>v[20]
gremlin> g.V().has('name', 'hercules').out('father').out('father').values('name')
==>saturn
```

For a sanity check, it is usually good to look at the properties of each
return, not the assigned long id.
```groovy
gremlin> g.V().has('name', 'hercules').values('name')
==>hercules
gremlin> g.V().has('name', 'hercules').out('father').values('name')
==>jupiter
gremlin> g.V().has('name', 'hercules').out('father').out('father').values('name')
==>saturn
```

Note the related traversal that shows the entire father family tree
branch of Hercules. This more complicated traversal is provided in order
to demonstrate the flexibility and expressivity of the language. A
competent grasp of Gremlin provides the JanusGraph user the ability to
fluently navigate the underlying graph structure.
```groovy
gremlin> g.V().has('name', 'hercules').repeat(out('father')).emit().values('name')
==>jupiter
==>saturn
```

Some more traversal examples are provided below.
```groovy
gremlin> hercules = g.V().has('name', 'hercules').next()
==>v[1536]
gremlin> g.V(hercules).out('father', 'mother').label()
==>god
==>human
gremlin> g.V(hercules).out('battled').label()
==>monster
==>monster
==>monster
gremlin> g.V(hercules).out('battled').valueMap()
==>{name=nemean}
==>{name=hydra}
==>{name=cerberus}
```

Given that *The Graph of the Gods* only has one battler (Hercules),
another battler (for the sake of example) is added to the graph with
Gremlin showcasing how vertices and edges are added to the graph.
```groovy
gremlin> theseus = graph.addVertex('human')
==>v[3328]
gremlin> theseus.property('name', 'theseus')
==>null
gremlin> cerberus = g.V().has('name', 'cerberus').next()
==>v[2816]
gremlin> battle = theseus.addEdge('battled', cerberus, 'time', 22)
==>e[7eo-2kg-iz9-268][3328-battled->2816]
gremlin> battle.values('time')
==>22
```

When adding a vertex, an optional vertex label can be provided. An edge
label must be specified when adding edges. Properties as key-value pairs
can be set on both vertices and edges. When a property key is defined
with SET or LIST cardinality, `addProperty` must be used when adding a
respective property to a vertex.
```groovy
gremlin> g.V(hercules).as('h').out('battled').in('battled').where(neq('h')).values('name')
==>theseus
```

The example above has 4 chained functions: `out`, `in`, `except`, and
`values` (i.e. `name` is shorthand for `values('name')`). The function
signatures of each are itemized below, where `V` is vertex and `U` is
any object, where `V` is a subset of `U`.

1.  `out: V -> V`
2.  `in: V -> V`
3.  `except: U -> U`
4.  `values: V -> U`

When chaining together functions, the incoming type must match the
outgoing type, where `U` matches anything. Thus, the "co-battled/ally"
traversal above is correct.

!!! note
    The Gremlin overview presented in this section focused on the
    Gremlin-Groovy language implementation used in the Gremlin Console.
    Refer to [Connecting to JanusGraph](../connecting/index.md) for information about connecting to
    JanusGraph with other languages than Groovy and independent of the
    Gremlin Console.

Iterating the Traversal
-----------------------

- `iterate()` - Zero results are expected or can be ignored.
- `next()` - Get one result. Make sure to check `hasNext()` first.
- `next(int n)` - Get the next `n` results. Make sure to check `hasNext()` first.
- `toList()` - Get all results as a list. If there are no results, an empty list is returned.

A Java code example is shown below to demonstrate these concepts:
```java
Traversal t = g.V().has("name", "pluto"); // Define a traversal
// Note the traversal is not executed/iterated yet
Vertex pluto = null;
if (t.hasNext()) { // Check if results are available
    pluto = g.V().has("name", "pluto").next(); // Get one result
    g.V(pluto).drop().iterate(); // Execute a traversal to drop pluto from graph
}
// Note the traversal can be cloned for reuse
Traversal tt = t.asAdmin().clone();
if (tt.hasNext()) {
    System.err.println("pluto was not dropped!");
}
List<Vertex> gods = g.V().hasLabel("god").toList(); // Find all the gods
```