Elasticsearch
=============

> Elasticsearch is a distributed, RESTful search and analytics engine
> capable of solving a growing number of use cases. As the heart of the
> Elastic Stack, it centrally stores your data so you can discover the
> expected and uncover the unexpected.
>
> —  [Elasticsearch
> Overview](https://www.elastic.co/products/elasticsearch/)

JanusGraph supports [Elasticsearch](https://www.elastic.co/) as an index
backend. Here are some of the Elasticsearch features supported by
JanusGraph:

-   **Full-Text**: Supports all `Text` predicates to search for text
    properties that matches a given word, prefix or regular expression.
-   **Geo**: Supports all `Geo` predicates to search for geo properties
    that are intersecting, within, disjoint to or contained in a given
    query geometry. Supports points, circles, boxes, lines and polygons
    for indexing. Supports circles, boxes and polygons for querying
    point properties and all shapes for querying non-point properties.
-   **Numeric Range**: Supports all numeric comparisons in `Compare`.
-   **Flexible Configuration**: Supports remote operation and open-ended
    settings customization.
-   **Collections**: Supports indexing SET and LIST cardinality
    properties.
-   **Temporal**: Nanosecond granularity temporal indexing.
-   **Custom Analyzer**: Choose to use a custom analyzer

Please see [Version Compatibility](../changelog.md#version-compatibility) for details on what versions of
Elasticsearch will work with JanusGraph.

!!! important
    Beginning with Elasticsearch 5.0 JanusGraph uses sandboxed [Painless scripts](https://www.elastic.co/guide/en/elasticsearch/reference/master/modules-scripting-painless.html)
    for inline updates, which are enabled by default in Elasticsearch 5.x.
    
    Using JanusGraph with Elasticsearch 2.x requires enabling Groovy
    inline scripting by setting `script.engine.groovy.inline.update` to
    `true` on the Elasticsearch cluster (see [dynamic scripting documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.3/modules-scripting.html#enable-dynamic-scripting)
    for more information).

Running Elasticsearch
---------------------

JanusGraph supports connections to a running Elasticsearch cluster.
JanusGraph provides two options for running local Elasticsearch
instances for getting started quickly. JanusGraph server (see
[Getting started](../basics/server.md#getting-started)) automatically starts a local
Elasticsearch instance. Alternatively JanusGraph releases include a full
Elasticsearch distribution to allow users to manually start a local
Elasticsearch instance (see [this
page](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html)
for more information).

```
$ elasticsearch/bin/elasticsearch
```

!!! note
    For security reasons Elasticsearch must be run under a non-root
    account

Elasticsearch Configuration Overview
------------------------------------

JanusGraph supports HTTP(S) client connections to a running
Elasticsearch cluster. Please see [Version Compatibility](../appendices.md#version-compatibility) for details on
what versions of Elasticsearch will work with the different client types
in JanusGraph.

!!! note
    JanusGraph’s index options start with the string "`index.[X].`" where
    "`[X]`" is a user-defined name for the backend. This user-defined name
    must be passed to JanusGraph’s ManagementSystem interface when
    building a mixed index, as described in [Mixed Index](../basics/index-performance.md#mixed-index), so that
    JanusGraph knows which of potentially multiple configured index
    backends to use. Configuration snippets in this chapter use the name
    `search`, whereas prose discussion of options typically write `[X]` in
    the same position. The exact index name is not significant as long as
    it is used consistently in JanusGraph’s configuration and when
    administering indices.

!!! tip
    It’s recommended that index names contain only alphanumeric lowercase
    characters and hyphens, and that they start with a lowercase letter.

### Connecting to Elasticsearch

The Elasticsearch client is specified as follows:
```conf
index.search.backend=elasticsearch
```

When connecting to Elasticsearch a single or list of hostnames for the
Elasticsearch instances must be provided. These are supplied via
JanusGraph’s `index.[X].hostname` key.
```conf
index.search.backend=elasticsearch
index.search.hostname=10.0.0.10:9200
```

Each host or host:port pair specified here will be added to the HTTP
client’s round-robin list of request targets. Here’s a minimal
configuration that will round-robin over 10.0.0.10 on the default
Elasticsearch HTTP port (9200) and 10.0.0.20 on port 7777:
```conf
index.search.backend=elasticsearch
index.search.hostname=10.0.0.10, 10.0.0.20:7777
```

#### JanusGraph `index.[X]` and `index.[X].elasticsearch` options

JanusGraph only uses default values for `index-name` and
`health-request-timeout`. See [Configuration Reference](../basics/configuration-reference.md) for descriptions of
these options and their accepted values.

-   `index.[X].elasticsearch.index-name`
-   `index.[X].elasticsearch.health-request-timeout`

### REST Client Options

The REST client accepts the `index.[X].bulk-refresh` option. This option
controls when changes are made visible to search. See [?refresh documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html)
for more information.

### Ingest Pipelines

If using Elasticsearch 5.0 or higher, a different ingest pipelines can
be set for each mixed index. Ingest pipeline can be use to pre-process
documents before indexing. A pipeline is composed by a series of
processors. Each processor transforms the document in some way. For
example [date processor](https://www.elastic.co/guide/en/elasticsearch/reference/current/date-processor.html)
can extract a date from a text to a date field. So you can query this
date with JanusGraph without it being physically in the primary storage.

-   `index.[X].elasticsearch.ingest-pipeline.[mixedIndexName] = pipeline_id`

See [ingest documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest.html)
for more information about ingest pipelines and [processors documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-processors.html)
for more information about ingest processors.

Secure Elasticsearch
--------------------

Elasticsearch does not perform authentication or authorization. A client
that can connect to Elasticsearch is trusted by Elasticsearch. When
Elasticsearch runs on an unsecured or public network, particularly the
Internet, it should be deployed with some type of external security.
This is generally done with a combination of firewalling, tunneling of
Elasticsearch’s ports or by using Elasticsearch extensions such as
[X-Pack](https://www.elastic.co/guide/en/x-pack/current/index.html).
Elasticsearch has two client-facing ports to consider:

-   The HTTP REST API, usually on port 9200
-   The native "transport" protocol, usually on port 9300

A client uses either one protocol/port or the other, but not both
simultaneously. Securing the HTTP protocol port is generally done with a
combination of firewalling and a reverse proxy with SSL encryption and
HTTP authentication. There are a couple of ways to approach security on
the native "transport" protocol port:

Tunnel Elasticsearch’s native "transport" protocol  
This approach can be implemented with SSL/TLS tunneling (for instance
via [stunnel](https://www.stunnel.org/index.html)), a VPN, or SSH port
forwarding. SSL/TLS tunnels require non-trivial setup and monitoring:
one or both ends of the tunnel need a certificate, and the stunnel
processes need to be configured and running continuously. The setup for
most secure VPNs is likewise non-trivial. Some Elasticsearch service
providers handle server-side tunnel management and provide a custom
Elasticsearch `transport.type` to simplify the client setup.

Add a firewall rule that allows only trusted clients to connect on Elasticsearch’s native protocol port  
This is typically done at the host firewall level. Easy to configure,
but very weak security by itself.

Index Creation Options
----------------------

JanusGraph supports customization of the index settings it uses when
creating its Elasticsearch index. It allows setting arbitrary key-value
pairs on the `settings` object in the [Elasticsearch `create index`
request](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html)
issued by JanusGraph. Here is a non-exhaustive sample of Elasticsearch
index settings that can be customized using this mechanism:

-   `index.number_of_replicas`
-   `index.number_of_shards`
-   `index.refresh_interval`

Settings customized through this mechanism are only applied when
JanusGraph attempts to create its index in Elasticsearch. If JanusGraph
finds that its index already exists, then it does not attempt to
recreate it, and these settings have no effect.

### Embedding Elasticsearch index creation settings with `create.ext`

JanusGraph iterates over all properties prefixed with
`index.[X].elasticsearch.create.ext.`, where `[X]` is an index name such
as `search`. It strips the prefix from each property key. The remainder
of the stripped key will be interpreted as an Elasticsearch index
creation setting. The value associated with the key is not modified. The
stripped key and unmodified value are passed as part of the `settings`
object in the Elasticsearch create index request that JanusGraph issues
when bootstrapping on Elasticsearch. This allows embedding arbitrary
index creation settings settings in JanusGraph’s properties. Here’s an
example configuration fragment that customizes three Elasticsearch index
settings using the `create.ext` config mechanism:
```conf
index.search.backend=elasticsearch
index.search.elasticsearch.create.ext.number_of_shards=15
index.search.elasticsearch.create.ext.number_of_replicas=3
index.search.elasticsearch.create.ext.shard.check_on_startup=true
```

The configuration fragment listed above takes advantage of
Elasticsearch’s assumption, implemented server-side, that unqualified
`create index` setting keys have an `index.` prefix. It’s also possible
to spell out the index prefix explicitly. Here’s a JanusGraph config
file functionally equivalent to the one listed above, except that the
`index.` prefix before the index creation settings is explicit:
```conf
index.search.backend=elasticsearch
index.search.elasticsearch.create.ext.index.number_of_shards=15
index.search.elasticsearch.create.ext.index.number_of_replicas=3
index.search.elasticsearch.create.ext.index.shard.check_on_startup=false
```

!!! tip
    The `create.ext` mechanism for specifying index creation settings is
    compatible with JanusGraph’s Elasticsearch configuration.

Troubleshooting
---------------

### Connection Issues to remote Elasticsearch cluster

Check that the Elasticsearch cluster nodes are reachable on the HTTP
protocol port from the JanusGraph nodes. Check the node listen port by
examining the Elasticsearch node configuration logs or using a general
diagnostic utility like `netstat`. Check the JanusGraph configuration.

Optimizing Elasticsearch
------------------------

### Write Optimization

For [bulk loading](../advanced-topics/bulk-loading.md) or other write-intense applications,
consider increasing Elasticsearch’s refresh interval. Refer to [this
discussion](https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-indexing-speed.html)
on how to increase the refresh interval and its impact on write
performance. Note, that a higher refresh interval means that it takes a
longer time for graph mutations to be available in the index.

For additional suggestions on how to increase write performance in
Elasticsearch with detailed instructions, please read [this blog post](http://blog.bugsense.com/post/35580279634/indexing-bigdata-with-elasticsearch).

### Further Reading

-   Please refer to the [Elasticsearch homepage](https://www.elastic.co)
    and available documentation for more information on Elasticsearch
    and how to setup an Elasticsearch cluster.
