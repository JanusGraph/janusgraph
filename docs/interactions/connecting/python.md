# Connecting from Python

Gremlin traversals can be constructed with Gremlin-Python just like in
Gremlin-Java or Gremlin-Groovy. Refer to [Gremlin Query Language](../../getting-started/gremlin.md) for an
introduction to Gremlin and pointers to further resources.

!!! important
    Some Gremlin step and predicate names are reserved words in Python.
    Those names are simply postfixed with `_` in Gremlin-Python, e.g.,
    `in()` becomes `in_()`, `not()` becomes `not_()`, and so on. The other
    names affected by this are: `all`, `and`, `as`, `from`, `global`,
    `is`, `list`, `or`, and `set`.

## Getting Started with JanusGraph and Gremlin-Python

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
from gremlin_python.process.anonymous_traversal_source import traversal

connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
# The connection should be closed on shut down to close open connections with connection.close()
g = traversal().withRemote(connection)
# Reuse 'g' across the application
```

3.  Execute a simple traversal:
```python
herculesAge = g.V().has('name', 'hercules').values('age').next()
print('Hercules is {} years old.'.format(herculesAge))
```
    `next()` is a terminal step that submits the traversal to the
    Gremlin Server and returns a single result.

## JanusGraph Specific Types and Predicates

JanusGraph contains some types and [predicates](../search-predicates.md) that
are not part of Apache TinkerPop and are therefore also not supported by
Gremlin-Python.
