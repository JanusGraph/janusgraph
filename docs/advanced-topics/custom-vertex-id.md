# Custom Vertex ID

JanusGraph has a built-in ID manager that allocates unique IDs for vertices, edges,
and properties. By default, all these IDs are numbers of long type, but JanusGraph allows
you to define custom vertex IDs.

## Configs

There are two boolean options you can control: `graph.set-vertex-id` and `graph.allow-custom-vid-types`.
Once the first option is enabled, you would be able to and must provide a vertex ID when
you create a vertex. The vertex ID must be long-type, unless you turn on the second option.
The second option allows to you either provide a long-type ID or a string-type ID. Note that
if you turn on `graph.allow-custom-vid-types`, you must also turn on `graph.set-vertex-id`.

Both of the two options are `GLOBAL_OFFLINE`, with default values being `false`. Being
global offline means once they are set, you would have to bring down the entire JanusGraph
cluster to alter the value. See the below two scenarios.

### Create a new Graph

Simply set `graph.set-vertex-id=<true/false>` and `graph.allow-custom-vid-types=<true/false>` in your config file.
When the graph is created, this value is loaded and persisted. After the graph is created,
changing the option value in the config file would make no difference as JanusGraph simply ignores the value.

### Alter an existing Graph

To alter a global offline config (not limited to the aforementioned two options), you need to shut
down all JanusGraph instances. Then open a gremlin console, connect to JanusGraph instance and run
the following:

```groovy
mgmt = graph.openManagement();
mgmt.set("graph.set-vertex-id", true);
// optional, if you want to provide string ID
mgmt.set("graph.allow-custom-vid-types", true);
mgmt.commit();
```

It will return an error if you have open JanusGraph instances. Otherwise, it will succeed and
now your global configs would have been updated.

## Custom Long ID

To provide a long vertex ID, you must call `graph.getIDManager().toVertexId(long)` to
retrieve a transformed JanusGraph ID. The input number must be positive. This is because
certain range of long IDs are reserved for internal usage, so you have to always call
the aforementioned utility to convert your ID. Unfortunately, this is not supported in
all gremlin clients as it is a JanusGraph specific utility. You can create vertices as follows:

```groovy
g = graph.traversal()
g.addV().property(T.id, graph.getIDManager().toVertexID(123L)).next()
g.tx().commit()
```

Note that if you want to get back the original ID you provided, you need to call
`graph.getIDManager().fromVertexID(long)`.

## Custom String ID

A string vertex ID can have arbitrary length as long as the underlying storage and index backends
permit. Elasticsearch, for example, requires the length of the ID to be smaller than or equal to
512.

The string value must consist of printable ASCII characters only, and it cannot contain
JanusGraph reserved relation delimiter (a single character string). By default, this reserved
delimiter is `-` (dash), which means you cannot use UUID string as a vertex ID since it
contains a `-` character. You can create vertices as follows:

```groovy
g = graph.traversal()
g.addV().property(T.id, "custom_vid_001").next()
g.tx().commit()
```

### Override reserved Character

If you create a new graph (using 1.0.0 or later version), JanusGraph allows you to set
`JANUSGRAPH_RELATION_DELIMITER` system property, a single character string which can be any
printable ASCII character. Once it is set, JanusGraph will prohibit that specific character
instead of the default `-` character. Alternatively, you can set `JANUSGRAPH_RELATION_DELIMITER`
environment variable, which is evaluated if and only if the `JANUSGRAPH_RELATION_DELIMITER`
system property is not present. For example, you can use the following command to set system
property when opening gremlin console:

```bash
JAVA_OPTIONS="-DJANUSGRAPH_RELATION_DELIMITER=@" ./bin/gremlin.sh
```

and then you should be able to use UUID as custom id since now the reserved character
becomes `@` rather than `-`.

```groovy
g = graph.traversal()
g.addV().property(T.id, UUID.randomUUID().toString()).next()
g.addV().property(T.id, "custom-vid-001").next()
g.tx().commit()
```

!!! warning
    If you decide to replace default `JANUSGRAPH_RELATION_DELIMITER`, you MUST
    always set the system property or environment value consistently across all
    JanusGraph instances, including servers and clients, during the entire lifecycle
    of the graph. Otherwise, data corruption could happen. For example, if you only
    set it on client side, then the server will not be able to deserialize any
    property or edge because of the setting mismatch. Also, you must not use this setting
    on a legacy graph. In other words, you cannot alter this property at any time.
