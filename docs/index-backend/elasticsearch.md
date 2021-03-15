# Elasticsearch

> Elasticsearch is a distributed, RESTful search and analytics engine
> capable of solving a growing number of use cases. As the heart of the
> Elastic Stack, it centrally stores your data so you can discover the
> expected and uncover the unexpected.
>
> —  [Elasticsearch
> Overview](https://www.elastic.co/elasticsearch/)

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

-   `index.[X].elasticsearch.index-name`
-   `index.[X].elasticsearch.health-request-timeout`

### REST Client Options

The REST client accepts the `index.[X].bulk-refresh` option. This option
controls when changes are made visible to search. See [?refresh documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html)
for more information.

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

### Write Optimization

For [bulk loading](../operations/bulk-loading.md) or other write-intense applications,
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
