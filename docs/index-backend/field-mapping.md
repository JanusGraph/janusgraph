# Field Mapping
## Individual Field Mapping

By default, JanusGraph will encode property keys to generate a unique
field name for the property key in the mixed index. If one wants to
query the mixed index directly in the external index backend can be
difficult to deal with and are illegible. For this use case, the field
name can be explicitly specified through a parameter.
```groovy
mgmt = graph.openManagement()
name = mgmt.makePropertyKey('bookname').dataType(String.class).make()
mgmt.buildIndex('booksBySummary', Vertex.class).addKey(name, Parameter.of('mapped-name', 'bookname')).buildMixedIndex("search")
mgmt.commit()
```

With this field mapping defined as a parameter, JanusGraph will use the
same name for the field in the `booksBySummary` index created in the
external index system as for the property key. Note, that it must be
ensured that the given field name is unique in the index.

## Global Field Mapping

Instead of individually adjusting the field mapping for every key added
to a mixed index, one can instruct JanusGraph to always set the field
name in the external index to be identical to the property key name.
This is accomplished by enabling the configuration option `map-name`
which is configured per indexing backend. If this option is enabled for
a particular indexing backend, then all mixed indexes defined against
said backend will use field names identical to the property key names.

However, this approach has two limitations: 1) The user has to ensure
that the property key names are valid field names for the indexing
backend and 2) renaming the property key will NOT rename the field name
in the index which can lead to naming collisions that the user has to be
aware of and avoid.

Note, that individual field mappings as described above can be used to
overwrite the default name for a particular key.

### Custom Analyzer

By default, JanusGraph will use the default analyzer from the indexing
backend for properties with Mapping.TEXT, and no analyzer for properties
with Mapping.STRING. If one wants to use another analyzer, it can be
explicitly specified through a parameter : ParameterType.TEXT\_ANALYZER
for Mapping.TEXT and ParameterType.STRING\_ANALYZER for Mapping.STRING.

#### For Elasticsearch

The name of the analyzer must be set as parameter value.
```groovy
mgmt = graph.openManagement()
string = mgmt.makePropertyKey('string').dataType(String.class).make()
text = mgmt.makePropertyKey('text').dataType(String.class).make()
textString = mgmt.makePropertyKey('textString').dataType(String.class).make()
mgmt.buildIndex('string', Vertex.class).addKey(string, Mapping.STRING.asParameter(), Parameter.of(ParameterType.STRING_ANALYZER.getName(), 'standard')).buildMixedIndex("search")
mgmt.buildIndex('text', Vertex.class).addKey(text, Mapping.TEXT.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), 'english')).buildMixedIndex("search")
mgmt.buildIndex('textString', Vertex.class).addKey(text, Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.STRING_ANALYZER.getName(), 'standard'), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), 'english')).buildMixedIndex("search")
mgmt.commit()
```

With these settings, JanusGraph will use the *standard* analyzer for
property key *string* and the *english* analyzer for property key
*text*.

#### For Solr

The class of the tokenizer must be set as parameter value.
```java
mgmt = graph.openManagement()
string = mgmt.makePropertyKey('string').dataType(String.class).make()
text = mgmt.makePropertyKey('text').dataType(String.class).make()
mgmt.buildIndex('string', Vertex.class).addKey(string, Mapping.STRING.asParameter(), Parameter.of(ParameterType.STRING_ANALYZER.getName(), 'org.apache.lucene.analysis.standard.StandardTokenizer')).buildMixedIndex("search")
mgmt.buildIndex('text', Vertex.class).addKey(text, Mapping.TEXT.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), 'org.apache.lucene.analysis.core.WhitespaceTokenizer')).buildMixedIndex("search")
mgmt.commit()
```

With these settings, JanusGraph will use the *standard* tokenizer for
property key *string* and the *whitespace* tokenizer for property key
*text*.


#### For Lucene

The name of the analyzer must be set as parameter value or it defaults to KeywordAnalyzer for `Mapping.STRING` and to StandardAnalyzer for `Mapping.TEXT`.

```java
mgmt = graph.openManagement()
string = mgmt.makePropertyKey('string').dataType(String.class).make()
text = mgmt.makePropertyKey('text').dataType(String.class).make()
name = mgmt.makePropertyKey('name').dataType(String.class).make()
document = mgmt.makePropertyKey('document').dataType(String.class).make()
mgmt.buildIndex('string', Vertex.class).addKey(string, Mapping.STRING.asParameter(), Parameter.of(ParameterType.STRING_ANALYZER.getName(), org.apache.lucene.analysis.core.SimpleAnalyzer.class.getName())).buildMixedIndex("search")
mgmt.buildIndex('text', Vertex.class).addKey(text, Mapping.TEXT.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), org.apache.lucene.analysis.en.EnglishAnalyzer.class.getName())).buildMixedIndex("search")
mgmt.buildIndex('name', Vertex.class).addKey(string, Mapping.STRING.asParameter()).buildMixedIndex("search")
mgmt.buildIndex('document', Vertex.class).addKey(text, Mapping.TEXT.asParameter()).buildMixedIndex("search")
mgmt.commit()
```

With these settings, JanusGraph will use a SimpleAnalyzer analyzer for property key `string`, an EnglishAnalyzer analyzer for property key `text`,  a KeywordAnalyzer analyzer for property `name` and a StandardAnalyzer analyzer for property 'document'.

### Custom parameters

Sometimes it is required to set additional parameters on mappings (other than mapping type, mapping name and analyzer). For example, when we would like to use a different similarity algorithm (to modify the scoring algorithm of full text search) or if we want to use a custom boosting on some fields in Elasticsearch we can set custom parameters (right now only Elasticsearch supports custom parameters).
The name of the custom parameter must be set through `ParameterType.customParameterName("yourProperty")`.

#### For Elasticsearch

```java
mgmt = graph.openManagement()
myProperty = mgmt.makePropertyKey('my_property').dataType(String.class).make()
mgmt.buildIndex('custom_property_test', Vertex.class).addKey(myProperty, Mapping.TEXT.asParameter(), Parameter.of(ParameterType.customParameterName("boost"), 5), Parameter.of(ParameterType.customParameterName("similarity"), "boolean")).buildMixedIndex("search")
mgmt.commit()
```

With these settings, JanusGraph will use the boost 5 and boolean similarity algorithm for property key `my_property`. Possible mapping parameters depend on Elasticsearch version. See [mapping parameters](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html) for current Elasticsearch version.