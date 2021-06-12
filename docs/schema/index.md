# Schema and Data Modeling

Each JanusGraph graph has a schema comprised of the edge labels,
property keys, and vertex labels used therein. A JanusGraph schema can
either be explicitly or implicitly defined. Users are encouraged to
explicitly define the graph schema during application development. An
explicitly defined schema is an important component of a robust graph
application and greatly improves collaborative software development.
Note, that a JanusGraph schema can be evolved over time without any
interruption of normal database operations. Extending the schema does
not slow down query answering and does not require database downtime.

The schema type - i.e. edge label, property key, or vertex label - is
assigned to elements in the graph - i.e. edge, properties or vertices
respectively - when they are first created. The assigned schema type
cannot be changed for a particular element. This ensures a stable type
system that is easy to reason about.

Beyond the schema definition options explained in this section, schema
types provide performance tuning options that are discussed in
[Advanced Schema](advschema.md).

## Displaying Schema Information

There are methods to view specific elements of the graph schema within the management API.
These methods are `mgmt.printIndexes()`, `mgmt.printPropertyKeys()`, `mgmt.printVertexLabels()`, and `mgmt.printEdgeLabels()`.
There is also a method that displays all the combined output named `printSchema()`.

```groovy
mgmt = graph.openManagement()
mgmt.printSchema()
```

## Defining Edge Labels

Each edge connecting two vertices has a label which defines the
semantics of the relationship. For instance, an edge labeled `friend`
between vertices A and B encodes a friendship between the two
individuals.

To define an edge label, call `makeEdgeLabel(String)` on an open graph
or management transaction and provide the name of the edge label as the
argument. Edge label names must be unique in the graph. This method
returns a builder for edge labels that allows to define its
multiplicity. The **multiplicity** of an edge label defines a
multiplicity constraint on all edges of this label, that is, a maximum
number of edges between pairs of vertices. JanusGraph recognizes the
following multiplicity settings.

### Edge Label Multiplicity

-   **MULTI**: Allows multiple edges of the same label between any pair
    of vertices. In other words, the graph is a *multi graph* with
    respect to such edge label. There is no constraint on edge
    multiplicity.
-   **SIMPLE**: Allows at most one edge of such label between any pair
    of vertices. In other words, the graph is a *simple graph* with
    respect to the label. Ensures that edges are unique for a given
    label and pairs of vertices.
-   **MANY2ONE**: Allows at most one outgoing edge of such label on any
    vertex in the graph but places no constraint on incoming edges. The
    edge label `mother` is an example with MANY2ONE multiplicity since
    each person has at most one mother but mothers can have multiple
    children.
-   **ONE2MANY**: Allows at most one incoming edge of such label on any
    vertex in the graph but places no constraint on outgoing edges. The
    edge label `winnerOf` is an example with ONE2MANY multiplicity since
    each contest is won by at most one person but a person can win
    multiple contests.
-   **ONE2ONE**: Allows at most one incoming and one outgoing edge of
    such label on any vertex in the graph. The edge label *marriedTo* is
    an example with ONE2ONE multiplicity since a person is married to
    exactly one other person.

The default multiplicity is MULTI. The definition of an edge label is
completed by calling the `make()` method on the builder which returns
the defined edge label as shown in the following example.
```groovy
mgmt = graph.openManagement()
follow = mgmt.makeEdgeLabel('follow').multiplicity(MULTI).make()
mother = mgmt.makeEdgeLabel('mother').multiplicity(MANY2ONE).make()
mgmt.commit()
```

## Defining Property Keys

Properties on vertices and edges are key-value pairs. For instance, the
property `name='Daniel'` has the key `name` and the value `'Daniel'`.
Property keys are part of the JanusGraph schema and can constrain the
allowed data types and cardinality of values.

To define a property key, call `makePropertyKey(String)` on an open
graph or management transaction and provide the name of the property key
as the argument. Property key names must be unique in the graph, and it
is recommended to avoid spaces or special characters in property names.
This method returns a builder for the property keys.

!!! note
    During property key creation, consider creating also graph indices for better 
    performance, see [Index Performance](../schema/index-management/index-performance.md).

### Property Key Data Type

Use `dataType(Class)` to define the data type of a property key.
JanusGraph will enforce that all values associated with the key have the
configured data type and thereby ensures that data added to the graph is
valid. For instance, one can define that the `name` key has a String
data type. Note that primitive types are not supported. Use the corresponding
wrapper class, e.g. `Integer` instead of `int`.

Define the data type as `Object.class` in order to allow any
(serializable) value to be associated with a key. However, it is
encouraged to use concrete data types whenever possible. Configured data
types must be concrete classes and not interfaces or abstract classes.
JanusGraph enforces class equality, so adding a sub-class of a
configured data type is not allowed.

JanusGraph natively supports the following data types.

<center>Native JanusGraph Data Types</center>

| Name | Description |
| ---- | ----- |
| String | Character sequence |
| Character | Individual character |
| Boolean | true or false |
| Byte | byte value |
| Short | short value |
| Integer | integer value |
| Long | long value |
| Float | 4 byte floating point number |
| Double | 8 byte floating point number |
| Date | Specific instant in time (`java.util.Date`) |
| Geoshape | Geographic shape like point, circle or box |
| UUID | Universally unique identifier (`java.util.UUID`) |

### Property Key Cardinality

Use `cardinality(Cardinality)` to define the allowed cardinality of the
values associated with the key on any given vertex.

-   **SINGLE**: Allows at most one value per element for such key. In
    other words, the key→value mapping is unique for all elements in the
    graph. The property key `birthDate` is an example with SINGLE
    cardinality since each person has exactly one birth date.
-   **LIST**: Allows an arbitrary number of values per element for such
    key. In other words, the key is associated with a list of values
    allowing duplicate values. Assuming we model sensors as vertices in
    a graph, the property key `sensorReading` is an example with LIST
    cardinality to allow lots of (potentially duplicate) sensor readings
    to be recorded.
-   **SET**: Allows multiple values but no duplicate values per element
    for such key. In other words, the key is associated with a set of
    values. The property key `name` has SET cardinality if we want to
    capture all names of an individual (including nick name, maiden
    name, etc).

The default cardinality setting is SINGLE. Note, that property keys used
on edges and properties have cardinality SINGLE. Attaching multiple
values for a single key on an edge or property is not supported.

```java
mgmt = graph.openManagement()
birthDate = mgmt.makePropertyKey('birthDate').dataType(Long.class).cardinality(Cardinality.SINGLE).make()
name = mgmt.makePropertyKey('name').dataType(String.class).cardinality(Cardinality.SET).make()
sensorReading = mgmt.makePropertyKey('sensorReading').dataType(Double.class).cardinality(Cardinality.LIST).make()
mgmt.commit()
```

## Relation Types

Edge labels and property keys are jointly referred to as **relation
types**. Names of relation types must be unique in the graph which means
that property keys and edge labels cannot have the same name. There are
methods in the JanusGraph API to query for the existence or retrieve
relation types which encompasses both property keys and edge labels.

```java
mgmt = graph.openManagement()
if (mgmt.containsRelationType('name'))
    name = mgmt.getPropertyKey('name')
mgmt.getRelationTypes(EdgeLabel.class)
mgmt.commit()
```

## Defining Vertex Labels

Like edges, vertices have labels. Unlike edge labels, vertex labels are
optional. Vertex labels are useful to distinguish different types of
vertices, e.g. *user* vertices and *product* vertices.

Although labels are optional at the conceptual and data model level,
JanusGraph assigns all vertices a label as an internal implementation
detail. Vertices created by the `addVertex` methods use JanusGraph’s
default label.

To create a label, call `makeVertexLabel(String).make()` on an open
graph or management transaction and provide the name of the vertex label
as the argument. Vertex label names must be unique in the graph.

```java
mgmt = graph.openManagement()
person = mgmt.makeVertexLabel('person').make()
mgmt.commit()
// Create a labeled vertex
person = graph.addVertex(label, 'person')
// Create an unlabeled vertex
v = graph.addVertex()
graph.tx().commit()
```

## Automatic Schema Maker

If an edge label, property key, or vertex label has not been defined
explicitly, it will be defined implicitly when it is first used during
the addition of an edge, vertex or the setting of a property. The
`DefaultSchemaMaker` configured for the JanusGraph graph defines such
types.

By default, implicitly created edge labels have multiplicity MULTI and
implicitly created property keys have cardinality SINGLE. Data types of
implicitly created property keys are inferred as long as they are natively
supported by JanusGraph. `Object.class` is used if and only if the given value
is not any of the `Native JanusGraph Data Types`.
Users can control automatic schema element creation by
implementing and registering their own `DefaultSchemaMaker`.

When defining a cardinality for a vertex property which differs from SINGLE, 
the cardinality should be used for all values of the vertex property in the 
first query (i.e. the query which defines a new vertex property key).

It is strongly encouraged to explicitly define all schema elements and
to disable automatic schema creation by setting `schema.default=none` in
the JanusGraph graph configuration.

## Changing Schema Elements

The definition of an edge label, property key, or vertex label cannot be
changed once its committed into the graph. However, the names of schema
elements can be changed via
`JanusGraphManagement.changeName(JanusGraphSchemaElement, String)` as
shown in the following example where the property key `place` is renamed
to `location`.

```java
mgmt = graph.openManagement()
place = mgmt.getPropertyKey('place')
mgmt.changeName(place, 'location')
mgmt.commit()
```

Note, that schema name changes may not be immediately visible in
currently running transactions and other JanusGraph graph instances in
the cluster. While schema name changes are announced to all JanusGraph
instances through the storage backend, it may take a while for the
schema changes to take effect and it may require a instance restart in
the event of certain failure conditions - like network partitions - if
they coincide with the rename. Hence, the user must ensure that either
of the following holds:

-   The renamed label or key is not currently in active use (i.e.
    written or read) and will not be in use until all JanusGraph
    instances are aware of the name change.
-   Running transactions actively accommodate the brief intermediate
    period where either the old or new name is valid based on the
    specific JanusGraph instance and status of the name-change
    announcement. For instance, that could mean transactions query for
    both names simultaneously.

Should the need arise to re-define an existing schema type, it is
recommended to change the name of this type to a name that is not
currently (and will never be) in use. After that, a new label or key can
be defined with the original name, thereby effectively replacing the old
one. However, note that this would not affect vertices, edges, or
properties previously written with the existing type. Redefining
existing graph elements is not supported online and must be accomplished
through a batch graph transformation.

Be careful that if you change a property name that is part of some
mixed index, you shall not reuse that property name in the same index.
For example, the following code will fail.

```java
name = mgmt.makePropertyKey("name").dataType(String.class).make()
mgmt.buildIndex("nameIndex", Vertex.class).addKey(name).buildMixedIndex("search")
mgmt.commit()

mgmt = graph.openManagement()
mgmt.changeName(mgmt.getPropertyKey("name"), "oldName");
name = mgmt.makePropertyKey("name").dataType(String.class).make()
mgmt.addIndexKey(mgmt.getGraphIndex("nameIndex"), name)
mgmt.commit()

// the following query will throw an exception
graph.traversal().V().has("name", textContains("value")).hasNext()
```

The reason is, JanusGraph does not attempt to alter the field name stored
in the mixed index backend when you call `mgmt.changeName`. Instead, JanusGraph
maintains a dual mapping between property name and index field name. If you
create a new index using the old name and add it to the same index, conflicts
will occur because JanusGraph cannot figure out which property should the index
field map to.

## Schema Constraints

The definition of the schema allows users to configure explicit property and connection constraints. Properties can be bound to specific vertex label and/or edge labels. Moreover, connection constraints allow users to explicitly define which two vertex labels can be connected by an edge label. These constraints can be used to ensure that a graph matches a given domain model. For example for the graph of the gods, a `god` can be a brother of another `god`, but not of a `monster` and a `god` can have a property `age`, but `location` can not have a property `age`. These constraints are disabled by default.

Enable these schema constraints by setting `schema.constraints=true`. This setting depends on the setting `schema.default`. If config `schema.default` is set to `none`, then an `IllegalArgumentException` is thrown for schema constraint violations. If `schema.default` is not set `none`, schema constraints are automatically created, but no exception is thrown.
Activating schema constraints has no impact on the existing data, because these schema constraints are only applied during the insertion process. So reading of data is not affected at all by those constraints.

Multiple properties can be bound to a vertex using `JanusGraphManagement.addProperties(VertexLabel, PropertyKey...)`, for example:

```java
mgmt = graph.openManagement()
person = mgmt.makeVertexLabel('person').make()
name = mgmt.makePropertyKey('name').dataType(String.class).cardinality(Cardinality.SET).make()
birthDate = mgmt.makePropertyKey('birthDate').dataType(Long.class).cardinality(Cardinality.SINGLE).make()
mgmt.addProperties(person, name, birthDate)
mgmt.commit()
```

Multiple properties can be bound to an edge using `JanusGraphManagement.addProperties(EdgeLabel, PropertyKey...)`, for example:

```java
mgmt = graph.openManagement()
follow = mgmt.makeEdgeLabel('follow').multiplicity(MULTI).make()
name = mgmt.makePropertyKey('name').dataType(String.class).cardinality(Cardinality.SET).make()
mgmt.addProperties(follow, name)
mgmt.commit()
```

Connections can be defined using `JanusGraphManagement.addConnection(EdgeLabel, VertexLabel out, VertexLabel in)` between an outgoing, an incoming and an edge, for example:

```java
mgmt = graph.openManagement()
person = mgmt.makeVertexLabel('person').make()
company = mgmt.makeVertexLabel('company').make()
works = mgmt.makeEdgeLabel('works').multiplicity(MULTI).make()
mgmt.addConnection(works, person, company)
mgmt.commit()
```

