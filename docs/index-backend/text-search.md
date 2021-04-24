# Index Parameters and Full-Text Search

When defining a mixed index, a list of parameters can be optionally
specified for each property key added to the index. These parameters
control how the particular key is to be indexed. JanusGraph recognizes
the following index parameters. Whether these are supported depends on
the configured index backend. A particular index backend might also
support custom parameters in addition to the ones listed here.

## Full-Text Search

When indexing string values, that is property keys with `String.class`
data type, one has the choice to either index those as text or character
strings which is controlled by the `mapping` parameter type.

When the value is indexed as text, the string is tokenized into a bag of
words which allows the user to efficiently query for all matches that
contain one or multiple words. This is commonly referred to as
**full-text search**. When the value is indexed as a character string,
the string is index "as-is" without any further analysis or
tokenization. This facilitates queries looking for an exact character
sequence match. This is commonly referred to as **string search**.

### Full-Text Search

By default, strings are indexed as text. To make this indexing option
explicit, one can define a mapping when indexing a property key as text.

```groovy
mgmt = graph.openManagement()
summary = mgmt.makePropertyKey('booksummary').dataType(String.class).make()
mgmt.buildIndex('booksBySummary', Vertex.class).addKey(summary, Mapping.TEXT.asParameter()).buildMixedIndex("search")
mgmt.commit()
```

This is identical to a standard mixed index definition with the only
addition of an extra parameter that specifies the mapping in the index -
in this case `Mapping.TEXT`.

When a string property is indexed as text, the string value is tokenized
into a bag of tokens. The exact tokenization depends on the indexing
backend and its configuration. JanusGraph’s default tokenization splits
the string on non-alphanumeric characters and removes any tokens with
less than 2 characters. The tokenization used by an indexing backend may
differ (e.g. stop words are removed) which can lead to minor differences
in how full-text search queries are handled for modifications inside a
transaction and committed data in the indexing backend.

When a string property is indexed as text, only full-text search
predicates are supported in graph queries by the indexing backend.
Full-text search is case-insensitive.

-   `textContains`: is true if (at least) one word inside the text
    string matches the query string
-   `textContainsPrefix`: is true if (at least) one word inside the text
    string begins with the query string
-   `textContainsRegex`: is true if (at least) one word inside the text
    string matches the given regular expression
-   `textContainsFuzzy`: is true if (at least) one word inside the text
    string is similar to the query String (based on Levenshtein edit
    distance)

```groovy
import static org.janusgraph.core.attribute.Text.*
g.V().has('booksummary', textContains('unicorns'))
g.V().has('booksummary', textContainsPrefix('uni'))
g.V().has('booksummary', textContainsRegex('.*corn.*'))
g.V().has('booksummary', textContainsFuzzy('unicorn'))
```

The Elasticsearch backend extends this functionality and includes support for negations
of the above predicates, as well as phrase matching:

- `textNotContains`: is true if no words inside the text string match the query string
- `textNotContainsPrefix`: is true if no words inside the text string begin with the query string
- `textNotContainsRegex`: is true if no words inside the text string match the given regular expression
- `textNotContainsFuzzy`:  is true if no words inside the text string are similar to the query string (based on Levenshtein edit distance)
- `textNotContainsPhrase`:  is true if the text string does not contain the sequence of words in the query string

String search predicates (see below) may be used in queries, but those
require filtering in memory which can be very costly.

### String Search

To index string properties as character sequences without any analysis
or tokenization, specify the mapping as `Mapping.STRING`:

```groovy
mgmt = graph.openManagement()
name = mgmt.makePropertyKey('bookname').dataType(String.class).make()
mgmt.buildIndex('booksBySummary', Vertex.class).addKey(name, Mapping.STRING.asParameter()).buildMixedIndex("search")
mgmt.commit()
```

When a string mapping is configured, the string value is indexed and can
be queried "as-is" - including stop words and non-letter characters.
However, in this case the query must match the entire string value.
Hence, the string mapping is useful when indexing short character
sequences that are considered to be one token.

When a string property is indexed as string, only the following
predicates are supported in graph queries by the indexing backend.
String search is case-sensitive.

-   `eq`: if the string is identical to the query string
-   `neq`: if the string is different than the query string
-   `textPrefix`: if the string value starts with the given query string
-   `textRegex`: if the string value matches the given regular
    expression in its entirety
-   `textFuzzy`: if the string value is similar to the given query
    string (based on Levenshtein edit distance)

```groovy
import static org.apache.tinkerpop.gremlin.process.traversal.P.*
import static org.janusgraph.core.attribute.Text.*
g.V().has('bookname', eq('unicorns'))
g.V().has('bookname', neq('unicorns'))
g.V().has('bookname', textPrefix('uni'))
g.V().has('bookname', textRegex('.*corn.*'))
g.V().has('bookname', textFuzzy('unicorn'))
```

The Elasticsearch backend extends this functionality and includes support for negations
of the above text predicates:

- `textNotPrefix`: if the string value does not start with the given query string
- `textNotRegex`: if the string value does not match the given regular expression in its entirety
- `textNotFuzzy`: if the string value is not similar to the given query string (based on Levenshtein edit distance)

Full-text search predicates may be used in queries, but those require
filtering in memory which can be very costly.

### Full text and string search

If you are using Elasticsearch it is possible to index properties as
both text and string allowing you to use all of the predicates for exact
and fuzzy matching.

```groovy
mgmt = graph.openManagement()
summary = mgmt.makePropertyKey('booksummary').dataType(String.class).make()
mgmt.buildIndex('booksBySummary', Vertex.class).addKey(summary, Mapping.TEXTSTRING.asParameter()).buildMixedIndex("search")
mgmt.commit()
```

Note that the data will be stored in the index twice, once for exact
matching and once for fuzzy matching.

## TinkerPop Text Predicates

It is also possible to use the TinkerPop text predicates with JanusGraph, but these predicates do not make use of
indices which means that they require filtering in memory which can be very costly.

```groovy
import static org.apache.tinkerpop.gremlin.process.traversal.TextP.*;
g.V().has('bookname', startingWith('uni'))
g.V().has('bookname', endingWith('corn'))
g.V().has('bookname', containing('nico'))
```

## Geo Mapping

By default, JanusGraph supports indexing geo properties with point type
and querying geo properties by circle or box. To index a non-point geo
property with support for querying by any geoshape type, specify the
mapping as `Mapping.PREFIX_TREE`:

```groovy
mgmt = graph.openManagement()
name = mgmt.makePropertyKey('border').dataType(Geoshape.class).make()
mgmt.buildIndex('borderIndex', Vertex.class).addKey(name, Mapping.PREFIX_TREE.asParameter()).buildMixedIndex("search")
mgmt.commit()
```

Additional parameters can be specified to tune the configuration of the
underlying prefix tree mapping. These optional parameters include the
number of levels used in the prefix tree as well as the associated
precision.

```groovy
mgmt = graph.openManagement()
name = mgmt.makePropertyKey('border').dataType(Geoshape.class).make()
mgmt.buildIndex('borderIndex', Vertex.class).addKey(name, Mapping.PREFIX_TREE.asParameter(), Parameter.of("index-geo-max-levels", 18), Parameter.of("index-geo-dist-error-pct", 0.0125)).buildMixedIndex("search")
mgmt.commit()
```

Note that some indexing backends (e.g. Solr) may require additional
external schema configuration to support and tune indexing non-point
properties.
