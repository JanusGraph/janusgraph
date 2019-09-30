<!--
NOTE: THIS FILE IS GENERATED VIA "mvn --quiet clean install -DskipTests=true -pl janusgraph-doc -am"
DO NOT EDIT IT DIRECTLY; CHANGES WILL BE OVERWRITTEN.
-->

### attributes.custom *
Custom attribute serialization and handling


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| attributes.custom.[X].attribute-class | Class of the custom attribute to be registered | String | (no default value) | GLOBAL_OFFLINE |
| attributes.custom.[X].serializer-class | Class of the custom attribute serializer to be registered | String | (no default value) | GLOBAL_OFFLINE |

### cache
Configuration options that modify JanusGraph's caching behavior


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| cache.db-cache | Whether to enable JanusGraph's database-level cache, which is shared across all transactions. Enabling this option speeds up traversals by holding hot graph elements in memory, but also increases the likelihood of reading stale data.  Disabling it forces each transaction to independently fetch graph elements from storage before reading/writing them. | Boolean | false | MASKABLE |
| cache.db-cache-clean-wait | How long, in milliseconds, database-level cache will keep entries after flushing them.  This option is only useful on distributed storage backends that are capable of acknowledging writes without necessarily making them immediately visible. | Integer | 50 | GLOBAL_OFFLINE |
| cache.db-cache-size | Size of JanusGraph's database level cache.  Values between 0 and 1 are interpreted as a percentage of VM heap, while larger values are interpreted as an absolute size in bytes. | Double | 0.3 | MASKABLE |
| cache.db-cache-time | Default expiration time, in milliseconds, for entries in the database-level cache. Entries are evicted when they reach this age even if the cache has room to spare. Set to 0 to disable expiration (cache entries live forever or until memory pressure triggers eviction when set to 0). | Long | 10000 | GLOBAL_OFFLINE |
| cache.tx-cache-size | Maximum size of the transaction-level cache of recently-used vertices. | Integer | 20000 | MASKABLE |
| cache.tx-dirty-size | Initial size of the transaction-level cache of uncommitted dirty vertices. This is a performance hint for write-heavy, performance-sensitive transactional workloads. If set, it should roughly match the median vertices modified per transaction. | Integer | (no default value) | MASKABLE |

### cluster
Configuration options for multi-machine deployments


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| cluster.max-partitions | The number of virtual partition blocks created in the partitioned graph. This should be larger than the maximum expected number of nodes in the JanusGraph graph cluster. Must be greater than 1 and a power of 2. | Integer | 32 | FIXED |

### computer
GraphComputer related configuration


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| computer.result-mode | How the graph computer should return the computed results. 'persist' for writing them into the graph, 'localtx' for writing them into the local transaction, or 'none' (default) | String | none | MASKABLE |

### graph
General configuration options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| graph.allow-stale-config | Whether to allow the local and storage-backend-hosted copies of the configuration to contain conflicting values for options with any of the following types: FIXED, GLOBAL_OFFLINE, GLOBAL.  These types are managed globally through the storage backend and cannot be overridden by changing the local configuration.  This type of conflict usually indicates misconfiguration.  When this option is true, JanusGraph will log these option conflicts, but continue normal operation using the storage-backend-hosted value for each conflicted option.  When this option is false, JanusGraph will log these option conflicts, but then it will throw an exception, refusing to start. | Boolean | true | MASKABLE |
| graph.allow-upgrade | Setting this to true will allow certain fixed values to be updated such as storage-version. This should only be used for upgrading. | Boolean | false | MASKABLE |
| graph.graphname | This config option is an optional configuration setting that you may supply when opening a graph. The String value you provide will be the name of your graph. If you use the ConfigurationManagement APIs, then you will be able to access your graph by this String representation using the ConfiguredGraphFactory APIs. | String | (no default value) | LOCAL |
| graph.replace-instance-if-exists | If a JanusGraph instance with the same instance identifier already exists, the usage of this configuration option results in the opening of this graph anwyay. | Boolean | false | LOCAL |
| graph.set-vertex-id | Whether user provided vertex ids should be enabled and JanusGraph's automatic id allocation be disabled. Useful when operating JanusGraph in concert with another storage system that assigns long ids but disables some of JanusGraph's advanced features which can lead to inconsistent data. EXPERT FEATURE - USE WITH GREAT CARE. | Boolean | false | FIXED |
| graph.storage-version | The version of JanusGraph storage schema with which this database was created. Automatically set on first start of graph. Should only ever be changed if upgraing to a new major release version of JanusGraph that contains schema changes | String | (no default value) | FIXED |
| graph.timestamps | The timestamp resolution to use when writing to storage and indices. Sets the time granularity for the entire graph cluster. To avoid potential inaccuracies, the configured time resolution should match those of the backend systems. Some JanusGraph storage backends declare a preferred timestamp resolution that reflects design constraints in the underlying service. When the backend provides a preferred default, and when this setting is not explicitly declared in the config file, the backend default is used and the general default associated with this setting is ignored.  An explicit declaration of this setting overrides both the general and backend-specific defaults. | TimestampProviders | MICRO | FIXED |
| graph.unique-instance-id | Unique identifier for this JanusGraph instance.  This must be unique among all instances concurrently accessing the same stores or indexes.  It's automatically generated by concatenating the hostname, process id, and a static (process-wide) counter. Leaving it unset is recommended. | String | (no default value) | LOCAL |
| graph.unique-instance-id-suffix | When this is set and unique-instance-id is not, this JanusGraph instance's unique identifier is generated by concatenating the hex encoded hostname to the provided number. | Short | (no default value) | LOCAL |
| graph.use-hostname-for-unique-instance-id | When this is set, this JanusGraph's unique instance identifier is set to the hostname. If unique-instance-id-suffix is also set, then the identifier is set to <hostname><suffix>. | Boolean | false | LOCAL |

### gremlin
Gremlin configuration options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| gremlin.graph | The implementation of graph factory that will be used by gremlin server | String | org.janusgraph.core.JanusGraphFactory | LOCAL |

### ids
General configuration options for graph element IDs


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| ids.block-size | Globally reserve graph element IDs in chunks of this size.  Setting this too low will make commits frequently block on slow reservation requests.  Setting it too high will result in IDs wasted when a graph instance shuts down with reserved but mostly-unused blocks. | Integer | 10000 | GLOBAL_OFFLINE |
| ids.flush | When true, vertices and edges are assigned IDs immediately upon creation.  When false, IDs are assigned only when the transaction commits. | Boolean | true | MASKABLE |
| ids.num-partitions | Number of partition block to allocate for placement of vertices | Integer | 10 | MASKABLE |
| ids.placement | Name of the vertex placement strategy or full class name | String | simple | MASKABLE |
| ids.renew-percentage | When the most-recently-reserved ID block has only this percentage of its total IDs remaining (expressed as a value between 0 and 1), JanusGraph asynchronously begins reserving another block. This helps avoid transaction commits waiting on ID reservation even if the block size is relatively small. | Double | 0.3 | MASKABLE |
| ids.renew-timeout | The number of milliseconds that the JanusGraph id pool manager will wait before giving up on allocating a new block of ids | Duration | 120000 ms | MASKABLE |
| ids.store-name | The name of the ID KCVStore. IDS_STORE_NAME is meant to be used only for backward compatibility with Titan, and should not be used explicitly in normal operations or in new graphs. | String | janusgraph_ids | GLOBAL_OFFLINE |

### ids.authority
Configuration options for graph element ID reservation/allocation


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| ids.authority.conflict-avoidance-mode | This setting helps separate JanusGraph instances sharing a single graph storage backend avoid contention when reserving ID blocks, increasing overall throughput. | ConflictAvoidanceMode | NONE | GLOBAL_OFFLINE |
| ids.authority.conflict-avoidance-tag | Conflict avoidance tag to be used by this JanusGraph instance when allocating IDs | Integer | 0 | LOCAL |
| ids.authority.conflict-avoidance-tag-bits | Configures the number of bits of JanusGraph-assigned element IDs that are reserved for the conflict avoidance tag | Integer | 4 | FIXED |
| ids.authority.randomized-conflict-avoidance-retries | Number of times the system attempts ID block reservations with random conflict avoidance tags before giving up and throwing an exception | Integer | 5 | MASKABLE |
| ids.authority.wait-time | The number of milliseconds the system waits for an ID block reservation to be acknowledged by the storage backend | Duration | 300 ms | GLOBAL_OFFLINE |

### index *
Configuration options for the individual indexing backends


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].backend | The indexing backend used to extend and optimize JanusGraph's query functionality. This setting is optional.  JanusGraph can use multiple heterogeneous index backends.  Hence, this option can appear more than once, so long as the user-defined name between "index" and "backend" is unique among appearances.Similar to the storage backend, this should be set to one of JanusGraph's built-in shorthand names for its standard index backends (shorthands: lucene, elasticsearch, es, solr) or to the full package and classname of a custom/third-party IndexProvider implementation. | String | elasticsearch | GLOBAL_OFFLINE |
| index.[X].conf-file | Path to a configuration file for those indexing backends that require/support a separate config file | String | (no default value) | MASKABLE |
| index.[X].directory | Directory to store index data locally | String | (no default value) | MASKABLE |
| index.[X].hostname | The hostname or comma-separated list of hostnames of index backend servers.  This is only applicable to some index backends, such as elasticsearch and solr. | String[] | 127.0.0.1 | MASKABLE |
| index.[X].index-name | Name of the index if required by the indexing backend | String | janusgraph | GLOBAL_OFFLINE |
| index.[X].map-name | Whether to use the name of the property key as the field name in the index. It must be ensured, that theindexed property key names are valid field names. Renaming the property key will NOT rename the field and its the developers responsibility to avoid field collisions. | Boolean | true | GLOBAL |
| index.[X].max-result-set-size | Maximum number of results to return if no limit is specified. For index backends that support scrolling, it represents the number of results in each batch | Integer | 50 | MASKABLE |
| index.[X].port | The port on which to connect to index backend servers | Integer | (no default value) | MASKABLE |

### index.[X].elasticsearch
Elasticsearch index configuration


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.bulk-refresh | Elasticsearch bulk API refresh setting used to control when changes made by this request are made visible to search | String | false | MASKABLE |
| index.[X].elasticsearch.health-request-timeout | When JanusGraph initializes its ES backend, JanusGraph waits up to this duration for the ES cluster health to reach at least yellow status.  This string should be formatted as a natural number followed by the lowercase letter "s", e.g. 3s or 60s. | String | 30s | MASKABLE |
| index.[X].elasticsearch.interface | Interface for connecting to Elasticsearch. TRANSPORT_CLIENT and NODE were previously supported, but now are required to migrate to REST_CLIENT. See the JanusGraph upgrade instructions for more details. | String | REST_CLIENT | MASKABLE |
| index.[X].elasticsearch.retry_on_conflict | Specify how many times should the operation be retried when a conflict occurs. | Integer | 0 | MASKABLE |
| index.[X].elasticsearch.scroll-keep-alive | How long (in seconds) elasticsearch should keep alive the scroll context. | Integer | 60 | GLOBAL_OFFLINE |
| index.[X].elasticsearch.setup-max-open-scroll-contexts | Whether JanusGraph should setup max_open_scroll_context to maximum value for the cluster or not. | Boolean | true | MASKABLE |
| index.[X].elasticsearch.use-all-field | Whether JanusGraph should add an "all" field mapping. When enabled field mappings will include a "copy_to" parameter referencing the "all" field. This is supported since Elasticsearch 6.x  and is required when using wildcard fields starting in Elasticsearch 6.x. | Boolean | true | GLOBAL_OFFLINE |
| index.[X].elasticsearch.use-mapping-for-es7 | Mapping types are deprecated in ElasticSearch 7 and JanusGraph will not use mapping types by default for ElasticSearch 7 but if you want to preserve mapping types, you can setup this parameter to true. If you are updating ElasticSearch from 6 to 7 and you don't want to reindex your indexes, you may setup this parameter to true but we do recommend to reindex your indexes and don't use this parameter. | Boolean | false | MASKABLE |

### index.[X].elasticsearch.create
Settings related to index creation


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.create.allow-mapping-update | Whether JanusGraph should allow a mapping update when registering an index. Only applicable when use-external-mappings is true. | Boolean | false | MASKABLE |
| index.[X].elasticsearch.create.sleep | How long to sleep, in milliseconds, between the successful completion of a (blocking) index creation request and the first use of that index.  This only applies when creating an index in ES, which typically only happens the first time JanusGraph is started on top of ES. If the index JanusGraph is configured to use already exists, then this setting has no effect. | Long | 200 | MASKABLE |
| index.[X].elasticsearch.create.use-external-mappings | Whether JanusGraph should make use of an external mapping when registering an index. | Boolean | false | MASKABLE |

### index.[X].elasticsearch.http.auth
Configuration options for HTTP(S) authentication.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.http.auth.type | Authentication type to be used for HTTP(S) access. | String | NONE | LOCAL |

### index.[X].elasticsearch.http.auth.basic
Configuration options for HTTP(S) Basic authentication.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.http.auth.basic.password | Password for HTTP(S) authentication. | String |  | LOCAL |
| index.[X].elasticsearch.http.auth.basic.realm | Realm value for HTTP(S) authentication. If empty, any realm is accepted. | String |  | LOCAL |
| index.[X].elasticsearch.http.auth.basic.username | Username for HTTP(S) authentication. | String |  | LOCAL |

### index.[X].elasticsearch.http.auth.custom
Configuration options for custom HTTP(S) authenticator.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.http.auth.custom.authenticator-args | Comma-separated custom authenticator constructor arguments. | String[] |  | LOCAL |
| index.[X].elasticsearch.http.auth.custom.authenticator-class | Authenticator fully qualified class name. | String |  | LOCAL |

### index.[X].elasticsearch.ssl
Elasticsearch SSL configuration


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.ssl.allow-self-signed-certificates | Controls the accepting of the self-signed SSL certificates. | Boolean | false | LOCAL |
| index.[X].elasticsearch.ssl.disable-hostname-verification | Disables the SSL hostname verification if set to true. Hostname verification is enabled by default. | Boolean | false | LOCAL |
| index.[X].elasticsearch.ssl.enabled | Controls use of the SSL connection to Elasticsearch. | Boolean | false | LOCAL |

### index.[X].elasticsearch.ssl.keystore
Configuration options for SSL Keystore.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.ssl.keystore.keypassword | The password to access the key in the SSL Keystore. If the option is not present, the value of "storepassword" is used. | String |  | LOCAL |
| index.[X].elasticsearch.ssl.keystore.location | Marks the location of the SSL Keystore. | String |  | LOCAL |
| index.[X].elasticsearch.ssl.keystore.storepassword | The password to access SSL Keystore. | String |  | LOCAL |

### index.[X].elasticsearch.ssl.truststore
Configuration options for SSL Truststore.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.ssl.truststore.location | Marks the location of the SSL Truststore. | String |  | LOCAL |
| index.[X].elasticsearch.ssl.truststore.password | The password to access SSL Truststore. | String |  | LOCAL |

### index.[X].solr
Solr index configuration


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].solr.configset | If specified, the same solr configSet can be reused for each new Collection that is created in SolrCloud. | String | (no default value) | MASKABLE |
| index.[X].solr.dyn-fields | Whether to use dynamic fields (which appends the data type to the field name). If dynamic fields is disabledthe user must map field names and define them explicitly in the schema. | Boolean | true | GLOBAL_OFFLINE |
| index.[X].solr.http-compression | Enable/disable compression on the HTTP connections made to Solr. | Boolean | false | MASKABLE |
| index.[X].solr.http-connection-timeout | Solr HTTP connection timeout. | Integer | 5000 | MASKABLE |
| index.[X].solr.http-max | Maximum number of HTTP connections in total to all Solr servers. | Integer | 100 | MASKABLE |
| index.[X].solr.http-max-per-host | Maximum number of HTTP connections per Solr host. | Integer | 20 | MASKABLE |
| index.[X].solr.http-urls | List of URLs to use to connect to Solr Servers (LBHttpSolrClient is used), don't add core or collection name to the URL. | String[] | http://localhost:8983/solr | MASKABLE |
| index.[X].solr.kerberos-enabled | Whether SOLR instance is Kerberized or not. | Boolean | false | MASKABLE |
| index.[X].solr.key-field-names | Field name that uniquely identifies each document in Solr. Must be specified as a list of `collection=field`. | String[] | (no default value) | GLOBAL |
| index.[X].solr.max-shards-per-node | Maximum number of shards per node. This applies when creating a new collection which is only supported under the SolrCloud operation mode. | Integer | 1 | GLOBAL_OFFLINE |
| index.[X].solr.mode | The operation mode for Solr which is either via HTTP (`http`) or using SolrCloud (`cloud`) | String | cloud | GLOBAL_OFFLINE |
| index.[X].solr.num-shards | Number of shards for a collection. This applies when creating a new collection which is only supported under the SolrCloud operation mode. | Integer | 1 | GLOBAL_OFFLINE |
| index.[X].solr.replication-factor | Replication factor for a collection. This applies when creating a new collection which is only supported under the SolrCloud operation mode. | Integer | 1 | GLOBAL_OFFLINE |
| index.[X].solr.ttl_field | Name of the TTL field for Solr collections. | String | ttl | GLOBAL_OFFLINE |
| index.[X].solr.wait-searcher | When mutating - wait for the index to reflect new mutations before returning. This can have a negative impact on performance. | Boolean | false | LOCAL |
| index.[X].solr.zookeeper-url | URL of the Zookeeper instance coordinating the SolrCloud cluster | String[] | localhost:2181 | MASKABLE |

### log *
Configuration options for JanusGraph's logging system


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| log.[X].backend | Define the log backed to use | String | default | GLOBAL_OFFLINE |
| log.[X].fixed-partition | Whether all log entries are written to one fixed partition even if the backend store is partitioned.This can cause imbalanced loads and should only be used on low volume logs | Boolean | false | GLOBAL_OFFLINE |
| log.[X].key-consistent | Whether to require consistency for log reading and writing messages to the storage backend | Boolean | false | MASKABLE |
| log.[X].max-partitions | The maximum number of partitions to use for logging. Setting up this many actual or virtual partitions. Must be bigger than 0and a power of 2. | Integer | (no default value) | FIXED |
| log.[X].max-read-time | Maximum time in ms to try reading log messages from the backend before failing. | Duration | 4000 ms | MASKABLE |
| log.[X].max-write-time | Maximum time in ms to try persisting log messages against the backend before failing. | Duration | 10000 ms | MASKABLE |
| log.[X].num-buckets | The number of buckets to split log entries into for load balancing | Integer | 1 | GLOBAL_OFFLINE |
| log.[X].read-batch-size | Maximum number of log messages to read at a time for logging implementations that read messages in batches | Integer | 1024 | MASKABLE |
| log.[X].read-interval | Time in ms between message readings from the backend for this logging implementations that read message in batch | Duration | 5000 ms | MASKABLE |
| log.[X].read-lag-time | Maximum time in ms that it may take for reads to appear in the backend. If a write does not becomevisible in the storage backend in this amount of time, a log reader might miss the message. | Duration | 500 ms | MASKABLE |
| log.[X].read-threads | Number of threads to be used in reading and processing log messages | Integer | 1 | MASKABLE |
| log.[X].send-batch-size | Maximum number of log messages to batch up for sending for logging implementations that support batch sending | Integer | 256 | MASKABLE |
| log.[X].send-delay | Maximum time in ms that messages can be buffered locally before sending in batch | Duration | 1000 ms | MASKABLE |
| log.[X].ttl | Sets a TTL on all log entries, meaningthat all entries added to this log expire after the configured amount of time. Requiresthat the log implementation supports TTL. | Duration | (no default value) | GLOBAL |

### metrics
Configuration options for metrics reporting


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| metrics.enabled | Whether to enable basic timing and operation count monitoring on backend | Boolean | false | MASKABLE |
| metrics.merge-stores | Whether to aggregate measurements for the edge store, vertex index, edge index, and ID store | Boolean | true | MASKABLE |
| metrics.prefix | The default name prefix for Metrics reported by JanusGraph. | String | org.janusgraph | MASKABLE |

### metrics.console
Configuration options for metrics reporting to console


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| metrics.console.interval | Time between Metrics reports printing to the console, in milliseconds | Duration | (no default value) | MASKABLE |

### metrics.csv
Configuration options for metrics reporting to CSV file


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| metrics.csv.directory | Metrics CSV output directory | String | (no default value) | MASKABLE |
| metrics.csv.interval | Time between dumps of CSV files containing Metrics data, in milliseconds | Duration | (no default value) | MASKABLE |

### metrics.ganglia
Configuration options for metrics reporting through Ganglia


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| metrics.ganglia.addressing-mode | Whether to communicate to Ganglia via uni- or multicast | String | unicast | MASKABLE |
| metrics.ganglia.hostname | The unicast host or multicast group name to which Metrics will send Ganglia data | String | (no default value) | MASKABLE |
| metrics.ganglia.interval | The number of milliseconds to wait between sending Metrics data to Ganglia | Duration | (no default value) | MASKABLE |
| metrics.ganglia.port | The port to which Ganglia data are sent | Integer | 8649 | MASKABLE |
| metrics.ganglia.protocol-31 | Whether to send data to Ganglia in the 3.1 protocol format | Boolean | true | MASKABLE |
| metrics.ganglia.spoof | If non-null, it must be a valid Gmetric spoof string formatted as an IP:hostname pair. See https://github.com/ganglia/monitor-core/wiki/Gmetric-Spoofing for information about this setting. | String | (no default value) | MASKABLE |
| metrics.ganglia.ttl | The multicast TTL to set on outgoing Ganglia datagrams | Integer | 1 | MASKABLE |
| metrics.ganglia.uuid | The host UUID to set on outgoing Ganglia datagrams. See https://github.com/ganglia/monitor-core/wiki/UUIDSources for information about this setting. | String | (no default value) | LOCAL |

### metrics.graphite
Configuration options for metrics reporting through Graphite


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| metrics.graphite.hostname | The hostname to receive Graphite plaintext protocol metric data | String | (no default value) | MASKABLE |
| metrics.graphite.interval | The number of milliseconds to wait between sending Metrics data | Duration | (no default value) | MASKABLE |
| metrics.graphite.port | The port to which Graphite data are sent | Integer | 2003 | MASKABLE |
| metrics.graphite.prefix | A Graphite-specific prefix for reported metrics | String | (no default value) | MASKABLE |

### metrics.jmx
Configuration options for metrics reporting through JMX


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| metrics.jmx.agentid | The JMX agentId used by Metrics | String | (no default value) | MASKABLE |
| metrics.jmx.domain | The JMX domain in which to report Metrics | String | (no default value) | MASKABLE |
| metrics.jmx.enabled | Whether to report Metrics through a JMX MBean | Boolean | false | MASKABLE |

### metrics.slf4j
Configuration options for metrics reporting through slf4j


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| metrics.slf4j.interval | Time between slf4j logging reports of Metrics data, in milliseconds | Duration | (no default value) | MASKABLE |
| metrics.slf4j.logger | The complete name of the Logger through which Metrics will report via Slf4j | String | (no default value) | MASKABLE |

### query
Configuration options for query processing


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| query.batch | Whether traversal queries should be batched when executed against the storage backend. This can lead to significant performance improvement if there is a non-trivial latency to the backend. | Boolean | false | MASKABLE |
| query.batch-property-prefetch | Whether to do a batched pre-fetch of all properties on adjacent vertices against the storage backend prior to evaluating a has condition against those vertices. Because these vertex properties will be loaded into the transaction-level cache of recently-used vertices when the condition is evaluated this can lead to significant performance improvement if there are many edges to adjacent vertices and there is a non-trivial latency to the backend. | Boolean | false | MASKABLE |
| query.fast-property | Whether to pre-fetch all properties on first singular vertex property access. This can eliminate backend calls on subsequentproperty access for the same vertex at the expense of retrieving all properties at once. This can be expensive for vertices with many properties | Boolean | true | MASKABLE |
| query.force-index | Whether JanusGraph should throw an exception if a graph query cannot be answered using an index. Doing solimits the functionality of JanusGraph's graph queries but ensures that slow graph queries are avoided on large graphs. Recommended for production use of JanusGraph. | Boolean | false | MASKABLE |
| query.ignore-unknown-index-key | Whether to ignore undefined types encountered in user-provided index queries | Boolean | false | MASKABLE |
| query.smart-limit | Whether the query optimizer should try to guess a smart limit for the query to ensure responsiveness in light of possibly large result sets. Those will be loaded incrementally if this option is enabled. | Boolean | true | MASKABLE |

### schema
Schema related configuration options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| schema.constraints | Configures the schema constraints to be used by this graph. If config 'schema.constraints' is set to 'true' and 'schema.default' is set to 'none', then an 'IllegalArgumentException' is thrown for schema constraint violations. If 'schema.constraints' is set to 'true' and 'schema.default' is not set 'none', schema constraints are automatically created as described in the config option 'schema.default'. If 'schema.constraints' is set to 'false' which is the default, then no schema constraints are applied. | Boolean | false | GLOBAL_OFFLINE |
| schema.default | Configures the DefaultSchemaMaker to be used by this graph. If set to 'none', automatic schema creation is disabled. Defaults to a blueprints compatible schema maker with MULTI edge labels and SINGLE property keys | String | default | MASKABLE |

### storage
Configuration options for the storage backend.  Some options are applicable only for certain backends.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.backend | The primary persistence provider used by JanusGraph.  This is required.  It should be set one of JanusGraph's built-in shorthand names for its standard storage backends (shorthands: berkeleyje, cassandrathrift, cassandra, astyanax, embeddedcassandra, cql, hbase, inmemory) or to the full package and classname of a custom/third-party StoreManager implementation. | String | (no default value) | LOCAL |
| storage.batch-loading | Whether to enable batch loading into the storage backend | Boolean | false | LOCAL |
| storage.buffer-size | Size of the batch in which mutations are persisted | Integer | 1024 | MASKABLE |
| storage.conf-file | Path to a configuration file for those storage backends which require/support a single separate config file. | String | (no default value) | LOCAL |
| storage.connection-timeout | Default timeout, in milliseconds, when connecting to a remote database instance | Duration | 10000 ms | MASKABLE |
| storage.directory | Storage directory for those storage backends that require local storage. | String | (no default value) | LOCAL |
| storage.drop-on-clear | Whether to drop the graph database (true) or delete rows (false) when clearing storage. Note that some backends always drop the graph database when clearing storage. Also note that indices are always dropped when clearing storage. | Boolean | true | MASKABLE |
| storage.hostname | The hostname or comma-separated list of hostnames of storage backend servers.  This is only applicable to some storage backends, such as cassandra and hbase. | String[] | 127.0.0.1 | LOCAL |
| storage.page-size | JanusGraph break requests that may return many results from distributed storage backends into a series of requests for small chunks/pages of results, where each chunk contains up to this many elements. | Integer | 100 | MASKABLE |
| storage.parallel-backend-ops | Whether JanusGraph should attempt to parallelize storage operations | Boolean | true | MASKABLE |
| storage.password | Password to authenticate against backend | String | (no default value) | LOCAL |
| storage.port | The port on which to connect to storage backend servers. For HBase, it is the Zookeeper port. | Integer | (no default value) | LOCAL |
| storage.read-only | Read-only database | Boolean | false | LOCAL |
| storage.read-time | Maximum time (in ms) to wait for a backend read operation to complete successfully. If a backend read operationfails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted.  | Duration | 10000 ms | MASKABLE |
| storage.root | Storage root directory for those storage backends that require local storage. If you do not supply storage.directory and you do supply graph.graphname, then your data will be stored in the directory equivalent to <STORAGE_ROOT>/<GRAPH_NAME>. | String | (no default value) | LOCAL |
| storage.setup-wait | Time in milliseconds for backend manager to wait for the storage backends to become available when JanusGraph is run in server mode | Duration | 60000 ms | MASKABLE |
| storage.transactions | Enables transactions on storage backends that support them | Boolean | true | MASKABLE |
| storage.username | Username to authenticate against backend | String | (no default value) | LOCAL |
| storage.write-time | Maximum time (in ms) to wait for a backend write operation to complete successfully. If a backend write operationfails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted.  | Duration | 100000 ms | MASKABLE |

### storage.berkeleyje
BerkeleyDB JE configuration options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.berkeleyje.cache-percentage | Percentage of JVM heap reserved for BerkeleyJE's cache | Integer | 65 | MASKABLE |
| storage.berkeleyje.isolation-level | The isolation level used by transactions | String | REPEATABLE_READ | MASKABLE |
| storage.berkeleyje.lock-mode | The BDB record lock mode used for read operations | String | LockMode.DEFAULT | MASKABLE |

### storage.cassandra
Cassandra storage backend options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cassandra.atomic-batch-mutate | True to use Cassandra atomic batch mutation, false to use non-atomic batches | Boolean | true | MASKABLE |
| storage.cassandra.compaction-strategy-class | The compaction strategy to use for JanusGraph tables | String | (no default value) | FIXED |
| storage.cassandra.compaction-strategy-options | Compaction strategy options.  This list is interpreted as a map.  It must have an even number of elements in [key,val,key,val,...] form. | String[] | (no default value) | FIXED |
| storage.cassandra.compression | Whether the storage backend should use compression when storing the data | Boolean | true | FIXED |
| storage.cassandra.compression-block-size | The size of the compression blocks in kilobytes | Integer | 64 | FIXED |
| storage.cassandra.compression-type | The sstable_compression value JanusGraph uses when creating column families. This accepts any value allowed by Cassandra's sstable_compression option. Leave this unset to disable sstable_compression on JanusGraph-created CFs. | String | LZ4Compressor | MASKABLE |
| storage.cassandra.frame-size-mb | The thrift frame size in megabytes | Integer | 15 | MASKABLE |
| storage.cassandra.keyspace | The name of JanusGraph's keyspace.  It will be created if it does not exist. If it is not supplied, but graph.graphname is, then the the keyspace will be set to that. | String | janusgraph | LOCAL |
| storage.cassandra.read-consistency-level | The consistency level of read operations against Cassandra | String | QUORUM | MASKABLE |
| storage.cassandra.replication-factor | The number of data replicas (including the original copy) that should be kept. This is only meaningful for storage backends that natively support data replication. | Integer | 1 | GLOBAL_OFFLINE |
| storage.cassandra.replication-strategy-class | The replication strategy to use for JanusGraph keyspace | String | org.apache.cassandra.locator.SimpleStrategy | FIXED |
| storage.cassandra.replication-strategy-options | Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a map.  It must have an even number of elements in [key,val,key,val,...] form.  A replication_factor set here takes precedence over one set with storage.cassandra.replication-factor | String[] | (no default value) | FIXED |
| storage.cassandra.write-consistency-level | The consistency level of write operations against Cassandra | String | QUORUM | MASKABLE |

### storage.cassandra.astyanax
Astyanax-specific Cassandra options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cassandra.astyanax.cluster-name | Default name for the Cassandra cluster | String | JanusGraph Cluster | MASKABLE |
| storage.cassandra.astyanax.connection-pool-type | Astyanax's connection pooler implementation | String | TOKEN_AWARE | MASKABLE |
| storage.cassandra.astyanax.frame-size | The thrift frame size in mega bytes | Integer | 15 | MASKABLE |
| storage.cassandra.astyanax.host-supplier | Host supplier to use when discovery type is set to DISCOVERY_SERVICE or TOKEN_AWARE | String | (no default value) | MASKABLE |
| storage.cassandra.astyanax.local-datacenter | The name of the local or closest Cassandra datacenter.  When set and not whitespace, this value will be passed into ConnectionPoolConfigurationImpl.setLocalDatacenter. When unset or set to whitespace, setLocalDatacenter will not be invoked. | String | (no default value) | MASKABLE |
| storage.cassandra.astyanax.max-cluster-connections-per-host | Maximum pooled "cluster" connections per host | Integer | 3 | MASKABLE |
| storage.cassandra.astyanax.max-connections | Maximum open connections allowed in the pool (counting all hosts) | Integer | -1 | MASKABLE |
| storage.cassandra.astyanax.max-connections-per-host | Maximum pooled connections per host | Integer | 32 | MASKABLE |
| storage.cassandra.astyanax.max-operations-per-connection | Maximum number of operations allowed per connection before the connection is closed | Integer | 100000 | MASKABLE |
| storage.cassandra.astyanax.node-discovery-type | How Astyanax discovers Cassandra cluster nodes | String | RING_DESCRIBE | MASKABLE |
| storage.cassandra.astyanax.read-page-size | The page size for Cassandra read operations | Integer | 4096 | MASKABLE |
| storage.cassandra.astyanax.retry-backoff-strategy | Astyanax's retry backoff strategy with configuration parameters | String | com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy,1000,5000 | MASKABLE |
| storage.cassandra.astyanax.retry-delay-slice | Astyanax's connection pool "retryDelaySlice" parameter | Integer | 10000 | MASKABLE |
| storage.cassandra.astyanax.retry-max-delay-slice | Astyanax's connection pool "retryMaxDelaySlice" parameter | Integer | 10 | MASKABLE |
| storage.cassandra.astyanax.retry-policy | Astyanax's retry policy implementation with configuration parameters | String | com.netflix.astyanax.retry.BoundedExponentialBackoff,100,25000,8 | MASKABLE |
| storage.cassandra.astyanax.retry-suspend-window | Astyanax's connection pool "retryMaxDelaySlice" parameter | Integer | 20000 | MASKABLE |

### storage.cassandra.ssl
Configuration options for SSL


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cassandra.ssl.enabled | Controls use of the SSL connection to Cassandra | Boolean | false | LOCAL |

### storage.cassandra.ssl.truststore
Configuration options for SSL Truststore.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cassandra.ssl.truststore.location | Marks the location of the SSL Truststore. | String |  | LOCAL |
| storage.cassandra.ssl.truststore.password | The password to access SSL Truststore. | String |  | LOCAL |

### storage.cassandra.thrift.cpool
Options for the Apache commons-pool connection manager


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cassandra.thrift.cpool.evictor-period | Approximate number of milliseconds between runs of the idle connection evictor.  Set to -1 to never run the idle connection evictor. | Long | 30000 | MASKABLE |
| storage.cassandra.thrift.cpool.idle-test | Whether the idle connection evictor validates idle connections and drops those that fail to validate | Boolean | false | MASKABLE |
| storage.cassandra.thrift.cpool.idle-tests-per-eviction-run | When the value is negative, e.g. -n, roughly one nth of the idle connections are tested per run.  When the value is positive, e.g. n, the min(idle-count, n) connections are tested per run. | Integer | 0 | MASKABLE |
| storage.cassandra.thrift.cpool.max-active | Maximum number of concurrently in-use connections (-1 to leave undefined) | Integer | 16 | MASKABLE |
| storage.cassandra.thrift.cpool.max-idle | Maximum number of concurrently idle connections (-1 to leave undefined) | Integer | 4 | MASKABLE |
| storage.cassandra.thrift.cpool.max-total | Max number of allowed Thrift connections, idle or active (-1 to leave undefined) | Integer | -1 | MASKABLE |
| storage.cassandra.thrift.cpool.max-wait | Maximum number of milliseconds to block when storage.cassandra.thrift.cpool.when-exhausted is set to BLOCK.  Has no effect when set to actions besides BLOCK.  Set to -1 to wait indefinitely. | Long | -1 | MASKABLE |
| storage.cassandra.thrift.cpool.min-evictable-idle-time | Minimum number of milliseconds a connection must be idle before it is eligible for eviction.  See also storage.cassandra.thrift.cpool.evictor-period.  Set to -1 to never evict idle connections. | Long | 60000 | MASKABLE |
| storage.cassandra.thrift.cpool.min-idle | Minimum number of idle connections the pool attempts to maintain | Integer | 0 | MASKABLE |
| storage.cassandra.thrift.cpool.when-exhausted | What to do when clients concurrently request more active connections than are allowed by the pool.  The value must be one of BLOCK, FAIL, or GROW. | String | BLOCK | MASKABLE |

### storage.cql
CQL storage backend options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.atomic-batch-mutate | True to use Cassandra atomic batch mutation, false to use non-atomic batches | Boolean | false | MASKABLE |
| storage.cql.batch-statement-size | The number of statements in each batch | Integer | 20 | MASKABLE |
| storage.cql.cluster-name | Default name for the Cassandra cluster | String | JanusGraph Cluster | MASKABLE |
| storage.cql.compact-storage | Whether the storage backend should use compact storage on tables. This option is only available for Cassandra 2 and earlier and defaults to true. | Boolean | true | FIXED |
| storage.cql.compaction-strategy-class | The compaction strategy to use for JanusGraph tables | String | (no default value) | FIXED |
| storage.cql.compaction-strategy-options | Compaction strategy options.  This list is interpreted as a map.  It must have an even number of elements in [key,val,key,val,...] form. | String[] | (no default value) | FIXED |
| storage.cql.compression | Whether the storage backend should use compression when storing the data | Boolean | true | FIXED |
| storage.cql.compression-block-size | The size of the compression blocks in kilobytes | Integer | 64 | FIXED |
| storage.cql.compression-type | The sstable_compression value JanusGraph uses when creating column families. This accepts any value allowed by Cassandra's sstable_compression option. Leave this unset to disable sstable_compression on JanusGraph-created CFs. | String | LZ4Compressor | MASKABLE |
| storage.cql.keyspace | The name of JanusGraph's keyspace.  It will be created if it does not exist. | String | janusgraph | LOCAL |
| storage.cql.local-core-connections-per-host | The number of connections initially created and kept open to each host for local datacenter | Integer | 1 | FIXED |
| storage.cql.local-datacenter | The name of the local or closest Cassandra datacenter.  When set and not whitespace, this value will be passed into ConnectionPoolConfigurationImpl.setLocalDatacenter. When unset or set to whitespace, setLocalDatacenter will not be invoked. | String | (no default value) | MASKABLE |
| storage.cql.local-max-connections-per-host | The maximum number of connections that can be created per host for local datacenter | Integer | 1 | FIXED |
| storage.cql.local-max-requests-per-connection | The maximum number of requests per connection for local datacenter | Integer | 1024 | FIXED |
| storage.cql.only-use-local-consistency-for-system-operations | True to prevent any system queries from using QUORUM consistency and always use LOCAL_QUORUM instead | Boolean | false | MASKABLE |
| storage.cql.protocol-version | The protocol version used to connect to the Cassandra database.  If no value is supplied then the driver will negotiate with the server. | Integer | 0 | LOCAL |
| storage.cql.read-consistency-level | The consistency level of read operations against Cassandra | String | QUORUM | MASKABLE |
| storage.cql.remote-core-connections-per-host | The number of connections initially created and kept open to each host for remote datacenter | Integer | 1 | FIXED |
| storage.cql.remote-max-connections-per-host | The maximum number of connections that can be created per host for remote datacenter | Integer | 1 | FIXED |
| storage.cql.remote-max-requests-per-connection | The maximum number of requests per connection for remote datacenter | Integer | 256 | FIXED |
| storage.cql.replication-factor | The number of data replicas (including the original copy) that should be kept | Integer | 1 | GLOBAL_OFFLINE |
| storage.cql.replication-strategy-class | The replication strategy to use for JanusGraph keyspace | String | SimpleStrategy | FIXED |
| storage.cql.replication-strategy-options | Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a map.  It must have an even number of elements in [key,val,key,val,...] form.  A replication_factor set here takes precedence over one set with storage.cql.replication-factor | String[] | (no default value) | FIXED |
| storage.cql.use-external-locking | True to prevent JanusGraph from using its own locking mechanism. Setting this to true eliminates redundant checks when using an external locking mechanism outside of JanusGraph. Be aware that when use-external-locking is set to true, that failure to employ a locking algorithm which locks all columns that participate in a transaction upfront and unlocks them when the transaction ends, will result in a 'read uncommitted' transaction isolation level guarantee. If set to true without an appropriate external locking mechanism in place side effects such as dirty/non-repeatable/phantom reads should be expected. | Boolean | false | MASKABLE |
| storage.cql.write-consistency-level | The consistency level of write operations against Cassandra | String | QUORUM | MASKABLE |

### storage.cql.ssl
Configuration options for SSL


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.ssl.enabled | Controls use of the SSL connection to Cassandra | Boolean | false | LOCAL |

### storage.cql.ssl.truststore
Configuration options for SSL Truststore.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.ssl.truststore.location | Marks the location of the SSL Truststore. | String |  | LOCAL |
| storage.cql.ssl.truststore.password | The password to access SSL Truststore. | String |  | LOCAL |

### storage.hbase
HBase storage options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.hbase.compat-class | The package and class name of the HBaseCompat implementation. HBaseCompat masks version-specific HBase API differences. When this option is unset, JanusGraph calls HBase's VersionInfo.getVersion() and loads the matching compat class at runtime.  Setting this option forces JanusGraph to instead reflectively load and instantiate the specified class. | String | (no default value) | MASKABLE |
| storage.hbase.compression-algorithm | An HBase Compression.Algorithm enum string which will be applied to newly created column families. The compression algorithm must be installed and available on the HBase cluster.  JanusGraph cannot install and configure new compression algorithms on the HBase cluster by itself. | String | GZ | MASKABLE |
| storage.hbase.region-count | The number of initial regions set when creating JanusGraph's HBase table | Integer | (no default value) | MASKABLE |
| storage.hbase.regions-per-server | The number of regions per regionserver to set when creating JanusGraph's HBase table | Integer | (no default value) | MASKABLE |
| storage.hbase.short-cf-names | Whether to shorten the names of JanusGraph's column families to one-character mnemonics to conserve storage space | Boolean | true | FIXED |
| storage.hbase.skip-schema-check | Assume that JanusGraph's HBase table and column families already exist. When this is true, JanusGraph will not check for the existence of its table/CFs, nor will it attempt to create them under any circumstances.  This is useful when running JanusGraph without HBase admin privileges. | Boolean | false | MASKABLE |
| storage.hbase.snapshot-name | The name of an exising HBase snapshot to be used by HBaseSnapshotInputFormat | String | janusgraph-snapshot | LOCAL |
| storage.hbase.snapshot-restore-dir | The tempoary directory to be used by HBaseSnapshotInputFormat to restore a snapshot. This directory should be on the same File System as the HBase root dir. | String | /tmp | LOCAL |
| storage.hbase.table | The name of the table JanusGraph will use.  When storage.hbase.skip-schema-check is false, JanusGraph will automatically create this table if it does not already exist. If this configuration option is not provided but graph.graphname is, the table will be set to that value. | String | janusgraph | LOCAL |

### storage.lock
Options for locking on eventually-consistent stores


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.lock.backend | Locker type to use | String | consistentkey | GLOBAL_OFFLINE |
| storage.lock.clean-expired | Whether to delete expired locks from the storage backend | Boolean | false | MASKABLE |
| storage.lock.expiry-time | Number of milliseconds after which a lock is considered to have expired. Lock applications that were not released are considered expired after this time and released. This value should be larger than the maximum time a transaction can take in order to guarantee that no correctly held applications are expired pre-maturely and as small as possible to avoid dead lock. | Duration | 300000 ms | GLOBAL_OFFLINE |
| storage.lock.local-mediator-group | This option determines the LocalLockMediator instance used for early detection of lock contention between concurrent JanusGraph graph instances within the same process which are connected to the same storage backend.  JanusGraph instances that have the same value for this variable will attempt to discover lock contention among themselves in memory before proceeding with the general-case distributed locking code.  JanusGraph generates an appropriate default value for this option at startup.  Overridding the default is generally only useful in testing. | String | (no default value) | LOCAL |
| storage.lock.retries | Number of times the system attempts to acquire a lock before giving up and throwing an exception | Integer | 3 | MASKABLE |
| storage.lock.wait-time | Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend. Also, the time waited at the end of all lock applications before verifying that the applications were successful. This value should be a small multiple of the average consistent write time. | Duration | 100 ms | GLOBAL_OFFLINE |

### storage.meta *
Meta data to include in storage backend retrievals


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.meta.[X].timestamps | Whether to include timestamps in retrieved entries for storage backends that automatically annotated entries with timestamps | Boolean | false | GLOBAL |
| storage.meta.[X].ttl | Whether to include ttl in retrieved entries for storage backends that support storage and retrieval of cell level TTL | Boolean | false | GLOBAL |
| storage.meta.[X].visibility | Whether to include visibility in retrieved entries for storage backends that support cell level visibility | Boolean | true | GLOBAL |

### tx
Configuration options for transaction handling


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| tx.log-tx | Whether transaction mutations should be logged to JanusGraph's write-ahead transaction log which can be used for recovery of partially failed transactions | Boolean | false | GLOBAL |
| tx.max-commit-time | Maximum time (in ms) that a transaction might take to commit against all backends. This is used by the distributed write-ahead log processing to determine when a transaction can be considered failed (i.e. after this time has elapsed).Must be longer than the maximum allowed write time. | Duration | 10000 ms | GLOBAL |

### tx.recovery
Configuration options for transaction recovery processes


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| tx.recovery.verbose | Whether the transaction recovery system should print recovered transactions and other activity to standard output | Boolean | false | MASKABLE |
