# Direct Index Query

JanusGraph’s standard global graph querying mechanism supports boolean
queries for vertices or edges. In other words, an element either matches
the query or it does not. There are no partial matches or result
scoring.

Some indexing backends additionally support fuzzy search queries. For
those queries, a score is computed for each match to indicate the
"goodness" of the match and results are returned in the order of their
score. Fuzzy search is particularly useful when dealing with full-text
search queries where matching more words is considered to be better.

Since fuzzy search implementations and scoring algorithms differ
significantly between indexing backends, JanusGraph does not support
fuzzy search natively. However, JanusGraph provides a *direct index
query* mechanism that allows search queries to be directly send to the
indexing backend for evaluation (for those backends that support it).

Use `Graph.indexQuery()` to compose a query that is executed directly
against an indexing backend. This query builder expects two parameters:

1.  The name of the indexing backend to query. This must be the name
    configured in JanusGraph’s configuration and used in the property
    key indexing definitions

2.  The query string

The builder allows configuration of the maximum number of elements to be
returned via its `limit(int)` method. The builder’s `offset(int)`
controls number of initial matches in the result set to skip. To
retrieve all vertex or edges matching the given query in the specified
indexing backend, invoke `vertices()` or `edges()`, respectively. It is
not possible to query for both vertices and edges at the same time.
These methods return an `Iterable` over `Result` objects. A result
object contains the matched handle, retrievable via `getElement()`, and
the associated score - `getScore()`.

Consider the following example:
```java
ManagementSystem mgmt = graph.openManagement();
PropertyKey text = mgmt.makePropertyKey("text").dataType(String.class).make();
mgmt.buildIndex("vertexByText", Vertex.class).addKey(text).buildMixedIndex("search");
mgmt.commit();
// ... Load vertices ...
for (Result<Vertex> result : graph.indexQuery("vertexByText", "v.text:(farm uncle berry)").vertices()) {
   System.out.println(result.getElement() + ": " + result.getScore());
}
```

## Query String

The query string is handed directly to the indexing backend for
processing and hence the query string syntax depends on what is
supported by the indexing backend. For vertex queries, JanusGraph will
analyze the query string for property key references starting with "v."
and replace those by a handle to the indexing field that corresponds to
the property key. Likewise, for edge queries, JanusGraph will replace
property key references starting with "e.". Hence, to refer to a
property of a vertex, use "v.\[KEY\_NAME\]" in the query string.
Likewise, for edges write "e.\[KEY\_NAME\]".

[Elasticsearch](elasticsearch.md) and [Lucene](lucene.md) support the
[Lucene query syntax](http://lucene.apache.org/core/4_10_4/queryparser/org/apache/lucene/queryparser/classic/package-summary.html).
Refer to the [Lucene documentation](http://lucene.apache.org/core/4_1_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html)
or the [Elasticsearch documentation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)
for more information. The query used in the example above follows the
Lucene query syntax.

    graph.indexQuery("vertexByText", "v.text:(farm uncle berry)").vertices()

This query matches all vertices where the text contains any of the three
words (grouped by parentheses) and score matches higher the more words
are matched in the text.

In addition [Elasticsearch](elasticsearch.md) supports wildcard queries,
use "v.\*" or "e.\*" in the query string to query if any of the
properties on the element match.

## Query Totals

It is sometimes useful to know how many total results were returned from
a query without having to retrieve all results. Fortunately,
Elasticsearch and Solr provide a shortcut that does not involve
retrieving and ranking all documents. This shortcut is exposed through
the ".vertexTotals()", ".edgeTotals()", and ".propertyTotals()" methods.

The totals can be retrieved using the same query syntax as the
indexQuery builder, but size is overwritten to be 0.
```groovy
graph.indexQuery("vertexByText", "v.text:(farm uncle berry)").vertexTotals()
```

## Gotchas

### Property Key Names

Names of property keys that contain non-alphanumeric characters must be
placed in quotation marks to ensure that the query is parsed correctly.
```groovy
graph.indexQuery("vertexByText", "v.\"first_name\":john").vertices()
```

Some property key names may be transformed by the JanusGraph indexing
backend implementation. For instance, an indexing backend that does not
permit spaces in field names may transform "My Field Name" to
"My•Field•Name", or an indexing backend like Solr may append type
information to the name, transforming "myBooleanField" to
"myBooleanField\_b". These transformations happen in the index backend’s
implementation of IndexProvider, in the "mapKey2Field" method. Indexing
backends may reserve special characters (such as *•*) and prohibit
indexing of fields that contain them. For this reason it is recommended
to avoid spaces and special characters in property names.

In general, making direct index queries depends on implementation
details of JanusGraph indexing backends that are normally hidden from
users, so it’s best to verify a query empirically against the indexing
backend in use.

### Element Identifier Collision

The strings "v.", "e.", and "p." are used to identify a vertex, edge or
property element respectively in a query. If the field name or the query
value contains the same sequence of characters, this can cause a
collision in the query string and parsing errors as in the following
example:
```groovy
graph.indexQuery("vertexByText", "v.name:v.john").vertices() //DOES NOT WORK!
```

To avoid such identifier collisions, use the `setElementIdentifier`
method to define a unique element identifier string that does not occur
in any other parts of the query:
```groovy
graph.indexQuery("vertexByText", "$v$name:v.john").setElementIdentifier("$v$").vertices()
```

### Mixed Index Availability Delay

When a query traverses a [mixed index](../schema/index-management/index-performance.md#mixed-index) immediately after
data is inserted the changes may not be visible. In
[Elasticsearch](elasticsearch.md) the configuration option that determines
this delay is [index refresh
interval](https://www.elastic.co/guide/en/elasticsearch/reference/5.4/index-modules.html#dynamic-index-settings).
In [Solr](solr.md) the primary configuration option is [max
time](https://lucene.apache.org/solr/guide/6_6/near-real-time-searching.html).
