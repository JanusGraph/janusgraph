Field Mapping
=============

Individual Field Mapping
------------------------

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

Global Field Mapping
--------------------

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
```groovy
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

