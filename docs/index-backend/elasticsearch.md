# Elasticsearch

> Elasticsearch is a distributed, RESTful search and analytics engine
> capable of solving a growing number of use cases. As the heart of the
> Elastic Stack, it centrally stores your data so you can discover the
> expected and uncover the unexpected.
>
> —  [Elasticsearch
> Overview](https://www.elastic.co/elasticsearch/)

JanusGraph supports [Elasticsearch](https://www.elastic.co/) as an index
backend, and [OpenSearch](#opensearch) with the same index backend. Here
are some of the Elasticsearch features supported by JanusGraph:

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
    JanusGraph uses sandboxed https://www.elastic.co/guide/en/elasticsearch/reference/master/modules-scripting-painless.html[Painless scripts] for inline updates, which are enabled by default in Elasticsearch.

## Running Elasticsearch

JanusGraph supports connections to a running Elasticsearch cluster.
JanusGraph provides two options for running local Elasticsearch
instances for getting started quickly. JanusGraph server (see
[Getting started](../operations/server.md#getting-started)) automatically starts a local
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

## Elasticsearch Configuration Overview

JanusGraph supports HTTP(S) client connections to a running
Elasticsearch cluster. Please see [Version Compatibility](../changelog.md#version-compatibility) for details on
what versions of Elasticsearch will work with the different client types
in JanusGraph.

!!! note
    JanusGraph’s index options start with the string "`index.[X].`" where
    "`[X]`" is a user-defined name for the backend. This user-defined name
    must be passed to JanusGraph’s ManagementSystem interface when
    building a mixed index, as described in [Mixed Index](../schema/index-management/index-performance.md#mixed-index), so that
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
```properties
index.search.backend=elasticsearch
```

When connecting to Elasticsearch a single or list of hostnames for the
Elasticsearch instances must be provided. These are supplied via
JanusGraph’s `index.[X].hostname` key.
```properties
index.search.backend=elasticsearch
index.search.hostname=10.0.0.10:9200
```

Each host or host:port pair specified here will be added to the HTTP
client’s round-robin list of request targets. Here’s a minimal
configuration that will round-robin over 10.0.0.10 on the default
Elasticsearch HTTP port (9200) and 10.0.0.20 on port 7777:
```properties
index.search.backend=elasticsearch
index.search.hostname=10.0.0.10, 10.0.0.20:7777
```

#### JanusGraph `index.[X]` and `index.[X].elasticsearch` options

JanusGraph only uses default values for `index-name` and
`health-request-timeout`. See [Configuration Reference](../configs/configuration-reference.md) for descriptions of
these options and their accepted values.

-   `index.[X].index-name`
-   `index.[X].elasticsearch.health-request-timeout`

### REST Client Options

The REST client accepts the `index.[X].elasticsearch.bulk-refresh` option. This option
controls when changes are made visible to search. See [?refresh documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html)
for more information.

The client of an index backend opens up to `index.[X].elasticsearch.max-connections` connections to all Elasticsearch
hosts together, 30 by default, and up to `index.[X].elasticsearch.max-connections-per-host` to each host, by default the
total divided evenly among the hosts, but at least 10. A request waits until a connection is free, so these cap how many
queries and bulk requests the index backend has in flight at once. A single host, such as a load balancer or the
endpoint of a hosted cluster, takes all 30 connections by default, while three hosts take 10 each, so that a host which
stops answering can't hold every connection. A JanusGraph Server which runs more Gremlin threads than that may need
higher values. Before JanusGraph 1.2.0 the client allowed 10 connections per host, whatever the number of hosts.

The client sends and receives with as many I/O threads as the JVM has processors, unless
`index.[X].elasticsearch.io-threads` sets another number. They don't wait for Elasticsearch, so a few are enough, which
matters for a JanusGraph Server with many graphs, as every index backend of every graph has a client of its own.

`index.[X].elasticsearch.compression=true` compresses the bodies of requests with gzip, bulk requests above all, and
accepts compressed responses, which saves network traffic at the cost of CPU on both sides. The client compresses a
request on one of its I/O threads, whose other requests wait meanwhile, so a large bulk request can delay queries. More
I/O threads make it less likely that a query shares that thread, and a smaller
`index.[X].elasticsearch.bulk-chunk-size-limit-bytes` shortens the wait. A compressed request is sent in chunks without
a Content-Length header, which some request signers, such as interceptors which sign requests with AWS Signature Version
4, may not handle.

A load balancer, NAT gateway or firewall between JanusGraph and Elasticsearch may drop a connection which stays idle
longer than its idle timeout, and the next request on that connection then fails. Set
`index.[X].elasticsearch.client-keep-alive` below that timeout, so that the client stops reusing a connection before the
network drops it.

### REST Client HTTPS Configuration

SSL support for HTTP can be enabled by setting the `index.[X].elasticsearch.ssl.enabled` configuration option to `true`. Note that depending on your configuration you may need to change the value of `index.[X].port` if your HTTPS port number is different from the default one for the REST API (9200).

When SSL is enabled you may also configure the location and password of the truststore. This can be done as follows:

```properties
index.search.elasticsearch.ssl.truststore.location=/path/to/your/truststore.jks
index.search.elasticsearch.ssl.truststore.password=truststorepwd
```

Note that these settings apply only to Elasticsearch REST client and do not affect any other SSL connections in JanusGraph.

Configuration of the client keystore is also supported:

```properties
index.search.elasticsearch.ssl.keystore.location=/path/to/your/keystore.jks
index.search.elasticsearch.ssl.keystore.storepassword=keystorepwd
index.search.elasticsearch.ssl.keystore.keypassword=keypwd
```

Any of the passwords can be empty.

If needed, the SSL hostname verification can be disabled by setting the `index.[X].elasticsearch.ssl.disable-hostname-verification` property value to `true` and the support for self-signed SSL certificates can be enabled by setting `index.[X].elasticsearch.ssl.allow-self-signed-certificates` property value to `true`.

!!! TIP
    It is not recommended to rely on the self-signed SSL certificates or to disable the hostname verification for a production system as it significantly limits the client's ability to provide the secure communication channel with the Elasticsearch server(s). This may result in leaking the confidential data which may be a part of your JanusGraph index.

### REST Client HTTP Authentication

REST client supports the following authentication options: Basic HTTP Authentication (username/password) and custom authentication based on the user-provided implementation.

These authentication methods are independent from SSL client authentication described above.

#### REST Client Basic HTTP Authentication

Basic HTTP Authentication is available regardless of the state of SSL support.  Optionally, an authentication realm can be specified via `index.[X].elasticsearch.http.auth.basic.realm` property.


```properties
index.search.elasticsearch.http.auth.type=basic
index.search.elasticsearch.http.auth.basic.username=httpuser
index.search.elasticsearch.http.auth.basic.password=httppassword
```

!!! tip
    It is highly recommended to use SSL (e.g. setting `index.[X].elasticsearch.ssl.enabled` to `true`) when using this option as the credentials can be intercepted when sent over an unencrypted connection!

#### REST Client Custom HTTP Authentication

Additional authentication methods can be implemented by providing your own implementation. The custom authenticator is configured as follows:

```properties
index.search.elasticsearch.http.auth.type=custom
index.search.elasticsearch.http.auth.custom.authenticator-class=fully.qualified.class.Name
index.search.elasticsearch.http.auth.custom.authenticator-args=arg1,arg2,...
```

Argument list is optional and can be empty.

The class specified there has to implement the `org.janusgraph.diskstorage.es.rest.util.RestClientAuthenticator` interface or extend `org.janusgraph.diskstorage.es.rest.util.RestClientAuthenticatorBase` convenience class. The implementation gets access to HTTP client configuration and can customize the client as needed. Refer to <<javadoc>> for more information.

For example, the following code snippet implements an authenticator allowing the
Elasticsearch REST client to authenticate and get authorized against AWS IAM:

```java
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.janusgraph.diskstorage.es.rest.util.RestClientAuthenticatorBase;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.google.common.base.Supplier;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;
/**
 * <p>
 * Elasticsearch REST HTTP(S) client callback implementing AWS request signing.
 * </p>
 * <p>
 * The signer is based on AWS SDK default provider chain, allowing multiple options for providing
 * the caller credentials. See {@link DefaultAWSCredentialsProviderChain} documentation for the details.
 * </p>
 */
public class AWSV4AuthHttpClientConfigCallback extends RestClientAuthenticatorBase {
    private static final String AWS_SERVICE_NAME = "es";
    private HttpRequestInterceptor awsSigningInterceptor;
    public AWSV4AuthHttpClientConfigCallback(final String[] args) {
        // does not require any configuration
    }
    @Override
    public void init() throws IOException {
        DefaultAWSCredentialsProviderChain awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final Supplier<LocalDateTime> clock = () -> LocalDateTime.now(ZoneOffset.UTC);
        // using default region provider chain
        // (https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/java-dg-region-selection.html)
        DefaultAwsRegionProviderChain regionProviderChain = new DefaultAwsRegionProviderChain();
        final String awsRegion = regionProviderChain.getRegion();
        final AWSSigner awsSigner = new AWSSigner(awsCredentialsProvider, awsRegion, AWS_SERVICE_NAME, clock);
        this.awsSigningInterceptor = new AWSSigningRequestInterceptor(awsSigner);
    }
    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        return httpClientBuilder.addInterceptorLast(awsSigningInterceptor);/
    }
}
```

This custom authenticator does not use any constructor arguments.

### Ingest Pipelines
Different ingest pipelines can be set for each mixed index.
Ingest pipeline can be use to pre-process documents before indexing. 
A pipeline is composed by a series of processors.
Each processor transforms the document in some way.
For example [date processor](https://www.elastic.co/guide/en/elasticsearch/reference/current/date-processor.html)
can extract a date from a text to a date field. So you can query this
date with JanusGraph without it being physically in the primary storage.

-   `index.[X].elasticsearch.ingest-pipeline.[mixedIndexName] = pipeline_id`

See [ingest documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest.html)
for more information about ingest pipelines and [processors documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-processors.html)
for more information about ingest processors.

## Secure Elasticsearch

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

A client uses either one protocol/port or the other, but not both simultaneously. Securing the HTTP protocol port is generally done with a combination of firewalling and a reverse proxy with SSL encryption and HTTP authentication. There are a couple of ways to approach security on the native "transport" protocol port:

In addition to that, some hosted Elasticsearch services offer other methods of authentication and authorization. For example, AWS Elasticsearch Service requires the use of HTTPS and offers an option for using IAM-based access control. For that the requests sent to this service must be signed. This can be achieved by using a custom authenticator (see above).

Tunnel Elasticsearch's native "transport" protocol:: This approach can be implemented with SSL/TLS tunneling (for instance via [stunnel](https://www.stunnel.org/index.html)), a VPN, or SSH port forwarding. SSL/TLS tunnels require non-trivial setup and monitoring: one or both ends of the tunnel need a certificate, and the stunnel processes need to be configured and running continuously. The setup for most secure VPNs is likewise non-trivial. Some Elasticsearch service providers handle server-side tunnel management and provide a custom Elasticsearch `transport.type` to simplify the client setup.

Add a firewall rule that allows only trusted clients to connect on Elasticsearch’s native protocol port  
This is typically done at the host firewall level. Easy to configure,
but very weak security by itself.

## OpenSearch

JanusGraph supports [OpenSearch](https://opensearch.org/) 2 and 3 with the `elasticsearch` index backend.
OpenSearch was forked from Elasticsearch 7.10 and provides the Elasticsearch 7 API, which JanusGraph uses
when the cluster reports an OpenSearch version:

```properties
index.search.backend=elasticsearch
index.search.hostname=localhost
```

JanusGraph rejects other OpenSearch versions like unsupported Elasticsearch versions, unless
`index.[X].elasticsearch.major-version` is set (see below). That includes OpenSearch 1, which reached its end
of life in May 2025.

When JanusGraph detects OpenSearch, it ignores `index.[X].elasticsearch.use-mapping-for-es7`, because
OpenSearch 2 removed mapping types.

OpenSearch doesn't need `compatibility.override_main_response_version` for JanusGraph, and OpenSearch 3
removed that setting. With it, OpenSearch 2 reports the version 7.10.2, so JanusGraph takes it for
Elasticsearch 7.

`index.[X].elasticsearch.major-version` sets the major version of the Elasticsearch API which the cluster
provides, so that JanusGraph doesn't ask the cluster for its version, for example if the JanusGraph user
may not read the root endpoint of the cluster. For OpenSearch, set it to `7`.

In both of these cases JanusGraph doesn't know that the cluster is OpenSearch, so keep
`index.[X].elasticsearch.use-mapping-for-es7` disabled.

The security plugin of OpenSearch is configured like a secured Elasticsearch cluster, with the
[HTTPS](#rest-client-https-configuration) and
[HTTP authentication](#rest-client-http-authentication) options:

```properties
index.search.elasticsearch.ssl.enabled=true
index.search.elasticsearch.http.auth.type=basic
index.search.elasticsearch.http.auth.basic.username=admin
index.search.elasticsearch.http.auth.basic.password=<password>
```

JanusGraph raises the cluster setting `search.max_open_scroll_context` when it opens the index. If the
JanusGraph user may not update cluster settings (`cluster:admin/settings/update`), set
`index.[X].elasticsearch.setup-max-open-scroll-contexts` to `false`. On Elasticsearch 7.12 and later a
result larger than a page is read through a point in time, which this setting doesn't limit. A scroll context
holds such a result on an older cluster, on OpenSearch, and on Elasticsearch where
`index.[X].elasticsearch.point-in-time` is `false`, where `index.[X].elasticsearch.major-version` is 7, or
where JanusGraph can't ask the cluster for its version. Such a context is released as soon as the result has
been read or its traversal is closed (see [Search Requests](#search-requests)), so the default limit of 500
open contexts per node is rarely reached.

Amazon OpenSearch Service domains which use IAM based access control need signed requests, which a
[custom authenticator](#rest-client-custom-http-authentication) can provide. These domains don't allow
changing `search.max_open_scroll_context`, so set `index.[X].elasticsearch.setup-max-open-scroll-contexts`
to `false` for them. Amazon OpenSearch Serverless isn't supported: it provides neither the scroll API nor
the stored scripts which JanusGraph uses.

## Index Creation Options

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

```properties
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
```properties
index.search.backend=elasticsearch
index.search.elasticsearch.create.ext.index.number_of_shards=15
index.search.elasticsearch.create.ext.index.number_of_replicas=3
index.search.elasticsearch.create.ext.index.shard.check_on_startup=false
```

!!! tip
    The `create.ext` mechanism for specifying index creation settings is
    compatible with JanusGraph’s Elasticsearch configuration.

## Troubleshooting

### Connection Issues to remote Elasticsearch cluster

Check that the Elasticsearch cluster nodes are reachable on the HTTP
protocol port from the JanusGraph nodes. Check the node listen port by
examining the Elasticsearch node configuration logs or using a general
diagnostic utility like `netstat`. Check the JanusGraph configuration.

## Optimizing Elasticsearch

### Search Requests

JanusGraph fetches the result of a mixed index query with as few requests as it can tell the result needs:

* A query whose offset and limit together are at most 10,000, Elasticsearch's default
  `index.max_result_window`, is one search request of exactly that size. Elasticsearch applies the offset of a
  [direct index query](direct-index-query.md) in that request.
* A query without a limit, or beyond that size, first asks for one hit more than a page after the offset, or
  for what is left up to the 10,000th hit if that is less. The page size is `index.[X].max-result-set-size`
  (50 by default). When fewer hits come back than were asked for, that is the whole result. Only a larger
  result is read in pages of that size, from its first hit on, which costs such a result one request more than
  the pages alone: on Elasticsearch 7.12 and later through a
  [point in time](https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html) and
  `search_after`, on an older cluster and on OpenSearch through the
  [scroll API](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#scroll-search-results).
  An offset of 10,000 or more leaves nothing to ask for first, and such a query is read in pages at once,
  however small its result.

A point in time holds the state of the index while the pages are read, so that they agree, and each page is
asked for after the last hit of the one before. A graph query without a limit and without an order reads its
pages in the order of the index, which Elasticsearch pages through without scoring. A limited graph query, which
JanusGraph may execute again with a larger limit, skipping the hits delivered so far, reads them in the order of
their scores, as its single request does and as a [direct index query](direct-index-query.md) which doesn't sort
does; that costs each page the scoring of the whole result, about twice the time of a scroll's page. A query
which sorts keeps its sort. Unlike a scroll
context, a point in time isn't counted against `search.max_open_scroll_context`, its pages don't count the total
number of hits, and a page which is sent again after a transient failure comes back the same, where a scroll may
skip it. `index.[X].elasticsearch.point-in-time` set to `false` makes every cluster scroll. A
cluster whose version JanusGraph doesn't ask for (`index.[X].elasticsearch.major-version` 7), or can't, scrolls
as well, since not every Elasticsearch 7 release has points in time, and so does OpenSearch: its point in time API
lacks the `_shard_doc` tiebreaker and the sort values for a search sorted by score which `search_after` needs
to page a query that doesn't sort by a field.

A point in time or a scroll context is released as soon as the result has been read to its end, the limit is
reached, or the traversal is closed, which JanusGraph Server does after every request. Embedded code which
abandons a traversal before its end should close it, for example with try-with-resources; otherwise the
context expires after `index.[X].elasticsearch.scroll-keep-alive` seconds (60 by default), as a scroll context
did before JanusGraph 1.2.0. A release which the cluster rejects, for example for want of the privilege, is
logged as a warning once. Neither the single request nor the first request of an unlimited query counts the
total number of hits, nor do the pages of a point in time; the pages of a scroll do, because Elasticsearch
requires it. On an index which JanusGraph did not create, `index.max_result_window` must allow 10,000 hits,
which is its default; JanusGraph lifts it on the indexes it creates.

If large results are common, a larger `index.[X].max-result-set-size` trades the size of one response for the
number of requests: a result of 5,000 hits is about 100 pages of 50, and one request when it is limited to
5,000.

### Write Optimization

For [bulk loading](../operations/bulk-loading.md) or other write-intense applications,
consider increasing Elasticsearch’s refresh interval. Refer to [this
discussion](https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-indexing-speed.html)
on how to increase the refresh interval and its impact on write
performance. Note, that a higher refresh interval means that it takes a
longer time for graph mutations to be available in the index.

### Reindex Optimization

Mixed-index reindex jobs can batch more documents per backend restore call than
the regular storage scan page size. This allows Elasticsearch to use larger bulk
requests during `SchemaAction.REINDEX` without changing `storage.page-size` for
other graph operations.

```properties
schema.reindex.mixed-index-batch-enabled = true
schema.reindex.mixed-index-batch-size = 1000
```

Increase `schema.reindex.mixed-index-batch-size` when Elasticsearch can handle
larger bulk requests and JanusGraph workers have enough memory for the queued
documents. Set `schema.reindex.mixed-index-batch-enabled` to `false` to keep
the previous storage-page-sized reindex batches.

#### Tuning the reindex throughput

The reindex job accumulates restored documents in memory and flushes them to
Elasticsearch in a single bulk (`_bulk`) request per worker once the batch size
is reached, so three settings determine reindex throughput:

-   **Batch size** (`schema.reindex.mixed-index-batch-size`). A larger batch
    sends fewer, larger bulk requests, which reduces the number of HTTP
    round-trips, transaction commits and management-system reads per reindexed
    element. The gains are largest going from the storage page size (the
    pre-batching default, e.g. 100) up to roughly one or a few thousand
    documents, and then taper off. Remember that a worker buffers up to
    `mixed-index-batch-size` documents in memory, and a reindex runs several
    workers in parallel, so peak heap usage grows with
    `mixed-index-batch-size` × reindex-threads × average-document-size — keep
    the default modest and raise it only when documents are small and workers
    have headroom.
-   **Reindex threads.** `updateIndex(index, SchemaAction.REINDEX)` uses
    `Runtime.getRuntime().availableProcessors()` worker threads by default; pass
    an explicit count with `updateIndex(index, SchemaAction.REINDEX, threads)`.
    More threads issue more concurrent bulk requests and scale best when the
    Elasticsearch index has multiple shards spread across data nodes.
-   **Bulk refresh** (`index.[X].elasticsearch.bulk-refresh`). With the default value `false`
    a reindex is throughput-bound. If it is set to `wait_for` (or `true`) every
    bulk request blocks until the next index refresh, which can dominate the
    total reindex time; in that mode batching helps the most, because it
    proportionally reduces the number of refresh waits. For the fastest reindex
    keep `bulk-refresh = false` and rely on the index becoming searchable at the
    next periodic refresh once the job completes.

#### When the storage scan, not Elasticsearch, is the ceiling

On the CQL backend the reindex pipeline is often **storage-scan-bound**, not
Elasticsearch-bound: the scan job streams the whole table through a small,
fixed number of scan queries (one per slice query — three for a vertex mixed
index), and the reindex worker threads spend most of their time waiting for
rows. The telltale signs are low Elasticsearch CPU, only a handful of reindex
workers reporting activity regardless of the configured thread count, and the
scan progress log (`StandardScannerExecutor`) showing a near-empty row queue.

Three CQL options remove that ceiling:

```properties
# Scan each Murmur3 token range on its own pipeline (true parallel scan).
storage.cql.parallel-scan-token-ranges = 8

# Bigger pages for full scans only (OLTP reads keep storage.page-size).
storage.cql.scan-page-size = 2000

# Stop scan queries server-side at each row's per-key limit (default true).
storage.cql.scan-per-partition-limit-enabled = true
```

-   **`parallel-scan-token-ranges`** splits the scan into disjoint token
    ranges, each drained by its own data-puller threads and merged
    independently — producer throughput scales roughly linearly until the
    Cassandra cluster saturates. Start with a small multiple of the cluster's
    node count and watch node load.
-   **`scan-page-size`** decouples the scan fetch size from the OLTP-oriented
    `storage.page-size`; a few thousand rows per page is usually a safe and
    substantial improvement. Pages are additionally prefetched one ahead, so
    the network wait overlaps row processing.
-   **`scan-per-partition-limit-enabled`** pushes each scan query's per-key
    limit into the query as `PER PARTITION LIMIT`. The scan's key-existence
    (grounding) query has a per-key limit of 1, so on graphs with wide rows
    (many edges or properties per vertex) this stops the grounding query from
    streaming the entire table to the client just to prove each key exists.
    On a CQL-compatible service without `PER PARTITION LIMIT` support the
    pushdown auto-disables with a warning at store open.

The scan-side and Elasticsearch-side settings compose: once the storage scan is
parallel, raise the reindex thread count and, if Elasticsearch becomes the new
bottleneck, scale its side (shards, replicas during rebuild, refresh interval)
as described above.

### Further Reading

-   Please refer to the [Elasticsearch homepage](https://www.elastic.co)
    and available documentation for more information on Elasticsearch
    and how to setup an Elasticsearch cluster.
