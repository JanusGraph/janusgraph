# Schema Initialization Strategies

Besides the ability to provide groovy code to JanusGraph server to initialize
schema or execute any custom logic, JanusGraph provides ability to execute 
custom schema initialization strategies on startup. 
These strategies are made to help users define and maintain their schema easier, 
but they are not replacement to custom schema management processes which can 
be developed by using `JanusGraphManagement` directly. 

Each time JanusGraph instance is started via `JanusGraphFactory` or `ConfiguredGraphFactory`, 
a schema initialization strategy gets executed before startup. 
Schema initialization strategy can be chosen via `schema.init.strategy` configuration parameter. 
By default `none` is selected, meaning schema initialization process is skipped.

Parameter `schema.init.schema-drop-before-startup` can be used to configure 
schema and data removal on startup (can be convenient in testing environments).

## JSON Schema Initialization

When `json` configuration option is selected in `schema.init.strategy` it 
triggers schema initialization via JSON formated schema definition which 
can be provided via file or string.

Configurations for this strategy are provided under `schema.init.json` namespace. 
The easiest way to set up JSON schema initialization it by providing JSON schema 
file path to `schema.init.json.file` or directly insert JSON schema into 
`schema.init.json.string` configuration option.

### JSON Schema Format

Schema defined via JSON must be a deserialized JSON version of 
`org.janusgraph.core.schema.json.definition.JsonSchemaDefinition` class where top
level object elements will be the following:

- `vertexLabels`
- `edgeLabels`
- `propertyKeys`
- `compositeIndexes`
- `vertexCentricEdgeIndexes`
- `vertexCentricPropertyIndexes`
- `mixedIndexes`

Each value is an array representing a list of vertex labels, edges labels, property 
keys, composite indexes, vertex-centric edge indexes, vertex-centric property indexes, 
or mixed indexes.

#### JSON Vertex Label Definition

Each vertex label object consists of the following keys:

- `label` - `string` datatype. (**Required**)
- `staticVertex` - `boolean` datatype.
- `partition` - `boolean` datatype.
- `ttl` - `number` datatype.

#### JSON Edge Label Definition

Each edge label object consists of the following keys:

- `label` - `string` datatype. (**Required**)
- `multiplicity` - `string` datatype. Allowed values: `MULTI`, `SIMPLE`, `ONE2MANY`, `MANY2ONE`, `ONE2ONE`.
- `unidirected` - `boolean` datatype.
- `ttl` - `number` datatype.

#### JSON Property Key Definition

Each property key object consists of the following keys:

- `key` - `string` datatype. (**Required**)
- `className` - `string` datatype. This must be a full class path of the selected property datatype. For example, `java.lang.String`, `java.lang.Long`,`org.janusgraph.core.attribute.Geoshape`. (**Required**)
- `cardinality` - `string` datatype. Allowed values: `SINGLE`, `LIST`, `SET`.
- `ttl` - `number` datatype.

#### JSON Composite Index Definition

Each composite index object consists of the following keys:

- `name` - `string` datatype. (**Required**)
- `typeClass` - `string` datatype. Full class path of the datatype of an indexed element. Allowed values: `org.apache.tinkerpop.gremlin.structure.Vertex`, `org.apache.tinkerpop.gremlin.structure.Edge`. (**Required**)
- `indexOnly` - `string` datatype. Vertex or Edge label.
- `unique` - `boolean` datatype.
- `consistency` - `string` datatype. Allowed values: `DEFAULT`, `LOCK`, `FORK`.
- `keys` - an array of objects representing `org.janusgraph.core.schema.json.definition.index.JsonIndexedPropertyKeyDefinition` (see definition below after indexes definition). These are used index keys. (**Required**)
- `inlinePropertyKeys` - an array of property keys to be inlined into the composite index. Currently supported for vertex composite indexes only. See [documentation](./index-management/index-performance.md#inlining-vertex-properties-into-a-composite-index) for more information about this feature.

#### JSON Mixed Index Definition

Each mixed index object consists of the following keys:

- `name` - `string` datatype. (**Required**)
- `typeClass` - `string` datatype. Full class path of the datatype of an indexed element. Allowed values: `org.apache.tinkerpop.gremlin.structure.Vertex`, `org.apache.tinkerpop.gremlin.structure.Edge`. (**Required**)
- `indexOnly` - `string` datatype. Vertex or Edge label.
- `indexBackend` - `string` datatype. Name for index backend configuration. (**Required**)
- `keys` - an array of objects representing `org.janusgraph.core.schema.json.definition.index.JsonIndexedPropertyKeyDefinition` (see definition below after indexes definition). These are used index keys. (**Required**)

#### JSON Vertex-Centric Edge Index Definition

Each vertex-centric edge index object consists of the following keys:

- `name` - `string` datatype. (**Required**)
- `propertyKeys` - an array of strings (each value has `string` datatype). These are the Edge properties which will be used for the index. (**Required**)
- `order` - `string` datatype. Allowed values: `asc`, `desc`.
- `indexedEdgeLabel` - `string` datatype. Edge label to be indexed. (**Required**)
- `direction` - `string` datatype. Allowed values: `OUT`, `IN`, `BOTH`.

#### JSON Vertex-Centric Property Index Definition

Each vertex-centric property index object consists of the following keys:

- `name` - `string` datatype. (**Required**)
- `propertyKeys` - an array of strings (each value has `string` datatype). These are the meta-properties which will be used for the index. (**Required**)
- `order` - `string` datatype. Allowed values: `asc`, `desc`.
- `indexedPropertyKey` - `string` datatype. Property key to be indexed. (**Required**)

#### JSON Definition of Property Keys defined in Composite and Mixed indexes (`keys`)

Each property key object defined as `keys` representation of composite or mixed index consists of the following keys:

- `propertyKey` - `string` datatype. (**Required**)
- `parameters` - an array of objects representing `org.janusgraph.core.schema.json.definition.JsonParameterDefinition`. These are optional parameters to let index know of additional configurations for the property key. (See description below)

#### JSON Definition of Parameters defined in Property Keys for Composite and Mixed indexes (`parameters`)

Each parameter is a configuration for to let underlying index backend configure relative properties better. Each such parameter consists of the following keys:

- `key` - `string` datatype. (**Required**)
- `value` - `string` datatype. (**Required**)
- `parser` - `string` datatype. This must be a full class pass of the parser which will be used to parse `value` of the parameter or a pre-defined shortcut. This parser must implement `org.janusgraph.core.schema.json.parser.JsonParameterParser` interface and have a parameterless constructor. If none is provided then `string` parser is used by default.

Pre-defined `parser` shortcuts:

- `string` - doesn't change `value` and uses it as is (`String` datatype).
- `enum` - replaces the provided string (defined as `<full class path>.<enum option>`) to actual enum value. 
For example, if `value` has a string `org.janusgraph.core.schema.Mapping.STRING` it will be replaced to actual `STRING` enum, 
and it won't be treated as a string.
- `boolean` - parses value to `Boolean`.
- `byte` - parses value to `Byte`.
- `short` - parses value to `Short`.
- `integer` - parses value to `Integer`.
- `long` - parses value to `Long`.
- `float` - parses value to `Float`.
- `double` - parses value to `Double`.

Parameters may include [custom keys](../index-backend/field-mapping.md#custom-parameters) defined as `ParameterType.customParameterName("<your custom key>")`. 
To define such keys in JSON it's necessary to use specific prefix for `key` - %\`custom%\` (in total the prefix consists of 10 characters. 
In case some characters are not rendered correctly here, you can always reference to `org.janusgraph.graphdb.types.ParameterType.CUSTOM_PARAMETER_PREFIX` 
to find out those 10 prefix characters).
As such, to define custom keys like `similarity` you should write your key as: %\`custom%\`similarity

### JSON Schema definition example

Following the rules above (defined in `JSON Schema Format`) an example of JSON schema definition could look like the one below.

!!! note
    Below schema is valid and tested using Cassandra and ElasticSearch. It uses some of the parameters related to 
    either eventual consistent databases and ElasticSearch in particular. Thus, some of the database specific schema definitions 
    may not work properly with other databases (like `similarity` or `string-analyzer`).
    The schema below is used for demonstration purposes only.

```json
{
    "vertexLabels": [
        {
            "label": "normalVertex"
        },
        {
            "label": "partitionedVertex",
            "partition": true
        },
        {
            "label": "unmodifiableVertex",
            "staticVertex": true
        },
        {
            "label": "temporaryVertexForTwoHours",
            "staticVertex": true,
            "ttl": 7200000
        }
    ],
    "edgeLabels": [
        {
            "label": "normalSimpleEdge",
            "multiplicity": "SIMPLE",
            "unidirected": false
        },
        {
            "label": "unidirectedMultiEdge",
            "multiplicity": "MULTI",
            "unidirected": true
        },
        {
            "label": "temporaryEdgeForOneHour",
            "ttl": 3600000
        },
        {
            "label": "edgeWhichUsesLocksInEventualConsistentDBs",
            "consistency": "LOCK"
        },
        {
            "label": "edgeWhichUsesForkingInEventualConsistentDBs",
            "consistency": "FORK"
        }
    ],
    "propertyKeys": [
        {
            "key": "normalProperty",
            "className": "java.lang.Long"
        },
        {
            "key": "stringProperty",
            "className": "java.lang.String",
            "cardinality": "SINGLE"
        },
        {
            "key": "anotherStringProperty",
            "className": "java.lang.String",
            "cardinality": "SINGLE"
        },
        {
            "key": "listProperty",
            "className": "java.lang.Long",
            "cardinality": "LIST"
        },
        {
            "key": "geoshapeProperty",
            "className": "org.janusgraph.core.attribute.Geoshape"
        },
        {
            "key": "setProperty",
            "className": "java.lang.String",
            "cardinality": "SET"
        },
        {
            "key": "propertyWhichUsesLocksInEventualConsistentDBs",
            "className": "java.lang.Long",
            "cardinality": "SET",
            "consistency": "LOCK"
        },
        {
            "key": "temporaryPropertyForOneHour",
            "className": "java.lang.Long",
            "ttl": 3600000
        }
    ],
    "compositeIndexes": [
        {
            "name": "simpleCompositeIndex",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Vertex",
            "keys": [
                {
                    "propertyKey": "normalProperty"
                }
            ]
        },
        {
            "name": "indexOnlyForNormalVerticesOnListProperty",
            "indexOnly": "normalVertex",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Vertex",
            "keys": [
                {
                    "propertyKey": "listProperty"
                }
            ]
        },
        {
            "name": "compositeIndexOnEdge",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Edge",
            "keys": [
                {
                    "propertyKey": "normalProperty"
                }
            ]
        },
        {
            "name": "uniqueCompositeIndexWithLocking",
            "indexOnly": "unmodifiableVertex",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Vertex",
            "unique": true,
            "consistency": "LOCK",
            "keys": [
                {
                    "propertyKey": "stringProperty"
                }
            ]
        },
        {
            "name": "multiKeysCompositeIndex",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Vertex",
            "keys": [
                {
                    "propertyKey": "normalProperty"
                },
                {
                    "propertyKey": "anotherStringProperty"
                }
            ]
        },
        {
            "name": "compositeIndexWithInlinedProperties",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Vertex",
            "keys": [
                {
                    "propertyKey": "setProperty"
                }
            ],
            "inlinePropertyKeys": ["normalProperty", "stringProperty", "anotherStringProperty"]
        }
    ],
    "vertexCentricEdgeIndexes": [
        {
            "name": "vertexCentricBothDirectionsEdgeIndex",
            "indexedEdgeLabel": "normalSimpleEdge",
            "direction": "BOTH",
            "propertyKeys": [
                "normalProperty"
            ],
            "order": "asc"
        },
        {
            "name": "vertexCentricUnidirectedEdgeIndexOnMultipleProperties",
            "indexedEdgeLabel": "unidirectedMultiEdge",
            "direction": "OUT",
            "propertyKeys": [
                "stringProperty",
                "anotherStringProperty"
            ],
            "order": "desc"
        }
    ],
    "vertexCentricPropertyIndexes": [
        {
            "name": "normalVertexCentricPropertyKey",
            "indexedPropertyKey": "listProperty",
            "propertyKeys": [
                "normalProperty"
            ],
            "order": "asc"
        }
    ],
    "mixedIndexes": [
        {
            "name": "simpleMixedIndexOnMultipleProperties",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Vertex",
            "indexBackend": "search",
            "keys": [
                {
                    "propertyKey": "normalProperty"
                },
                {
                    "propertyKey": "geoshapeProperty"
                }
            ]
        },
        {
            "name": "mixedIndexWithParametersOnProperties",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Vertex",
            "indexBackend": "search",
            "keys": [
                {
                    "propertyKey": "stringProperty",
                    "parameters": [
                        {
                            "key": "string-analyzer",
                            "value": "standard",
                            "parser": "string"
                        },
                        {
                            "key": "mapping",
                            "value": "org.janusgraph.core.schema.Mapping.STRING",
                            "parser": "enum"
                        }
                    ]
                }
            ]
        },
        {
            "name": "mixedIndexWithCustomParameterKeyAndParserFullClassPath",
            "indexOnly": "unidirectedMultiEdge",
            "typeClass": "org.apache.tinkerpop.gremlin.structure.Edge",
            "indexBackend": "search",
            "keys": [
                {
                    "propertyKey": "anotherStringProperty",
                    "parameters": [
                        {
                            "key": "%`custom%`similarity",
                            "value": "boolean",
                            "parser": "string"
                        },
                        {
                            "key": "mapping",
                            "value": "org.janusgraph.core.schema.Mapping.TEXTSTRING",
                            "parser": "org.janusgraph.core.schema.json.parser.EnumJsonParameterParser"
                        }
                    ]
                }
            ]
        }
    ]
}
```

### JSON schema initialization flow

Currently, JSON schema initialization flow is simple and doesn't include any schema update or schema migration features. 
The flow is split on multiple phases:

1) Creation of simple elements: `PropertyKey`, `VertexLabel`, `EdgeLabel`.
2) Creation of indices: Composite indexes, Vertex-centric Edge indexes, Vertex-centric Property indexes, Mixed indexes.
3) Indexes activation phase (configured via `schema.init.json.indices-activation`).

!!! note
    Current implementation of JSON schema importer only creates elements, but it never updates them even if you change 
    definition of your property, edge, vertex, or index. If an element with such name exists - schema importer will skip it 
    without doing any updates on the element. Also, it never deletes any existing schema elements from the graph.
    For schema migration and removal processes use `graph.openManagement()` directly.

Users who struggle to initialize schema due to created zombie JanusGraph instances in the cluster may leverage 
configuration option `schema.init.json.force-close-other-instances`. This option will automatically close all 
JanusGraph instance in the cluster (including activate instances). 

!!! warning
    When using `schema.init.json.force-close-other-instances` JanusGraph will force-close any other instances in 
    the cluster. However, they may not know about it and continue to work in the cluster without receiving any schema 
    updates. This could lead to split brain problem and corrupt current data. 
    It's advised to use `graph.unique-instance-id` and `graph.replace-instance-if-exists` options instead to prevent 
    creation of JanusGraph zombie instances.

### JSON Schema Definition API

It's also possible to manually trigger JSON schema definition API instead of relaying on startup schema 
initialization process. All helper methods for JSON schema initialization are located in 
`org.janusgraph.core.schema.JsonSchemaInitStrategy`.

```groovy
// Schema from file
JsonSchemaInitStrategy.initializeSchemaFromFile(graph, "/path/to/schema.json")

// Schema from string
JsonSchemaInitStrategy.initializeSchemaFromString(graph, "{ \"vertexLabels\": [ { \"label\": \"my_vertex\" } ] }")
```

Also, there are additional `initializeSchemaFromFile` and `initializeSchemaFromString` methods where it's possible to 
provide all configuration options directly instead of fetching them from the configuration of the graph:

- `initializeSchemaFromFile(JanusGraph graph, boolean createSchemaElements, boolean createSchemaIndices, IndicesActivationType indicesActivationType, boolean forceRollBackActiveTransactions, boolean forceCloseOtherInstances, long indexStatusTimeout, String jsonSchemaFilePath)`
- `initializeSchemaFromString(JanusGraph graph, boolean createSchemaElements, boolean createSchemaIndices, IndicesActivationType indicesActivationType, boolean forceRollBackActiveTransactions, boolean forceCloseOtherInstances, long indexStatusTimeout, String jsonSchemaString)`
