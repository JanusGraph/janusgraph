Apache Lucene
=============

> Apache Lucene is a high-performance, full-featured text search engine
> library written entirely in Java. It is a technology suitable for
> nearly any application that requires full-text search, especially
> cross-platform. Apache Lucene is an open source project available for
> free download.
>
> —  [Apache Lucene Homepage](http://lucene.apache.org/)

JanusGraph supports [Apache Lucene](http://lucene.apache.org/) as a
single-machine, embedded index backend. Lucene has a slightly extended
feature set and performs better in small-scale applications compared to
[Elasticsearch](elasticsearch.md), but is limited to single-machine
deployments.

Lucene Embedded Configuration
-----------------------------

For single machine deployments, Lucene runs embedded with JanusGraph.
JanusGraph starts and interfaces with Lucene internally.

To run Lucene embedded, add the following configuration options to the
graph configuration file where `/data/searchindex` specifies the
directory where Lucene should store the index data:
```conf
index.search.backend=lucene
index.search.directory=/data/searchindex
```

In the above configuration, the index backend is named `search`. Replace
`search` by a different name to change the name of the index.

Feature Support
---------------

-   **Full-Text**: Supports all `Text` predicates to search for text
    properties that matches a given word, prefix or regular expression.
-   **Geo**: Supports `Geo` predicates to search for geo properties that
    are intersecting, within, or contained in a given query geometry.
    Supports points, lines and polygons for indexing. Supports circles
    and boxes for querying point properties and all shapes for querying
    non-point properties.
-   **Numeric Range**: Supports all numeric comparisons in `Compare`.
-   **Temporal**: Nanosecond granularity temporal indexing.

Configuration Options
---------------------

Refer to [Configuration Reference](../basics/configuration-reference.md) for a complete listing of all Lucene
specific configuration options in addition to the general JanusGraph
configuration options.

Note, that each of the index backend options needs to be prefixed with
`index.[INDEX-NAME].` where `[INDEX-NAME]` stands for the name of the
index backend. For instance, if the index backend is named *search* then
these configuration options need to be prefixed with `index.search.`. To
configure an index backend named *search* to use Lucene as the index
system, set the following configuration option:
```conf
index.search.backend=lucene
```

Further Reading
---------------

-   Please refer to the [Apache Lucene
    homepage](http://lucene.apache.org/) and available documentation for
    more information on Lucene.
