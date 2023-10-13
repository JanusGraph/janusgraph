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
| cache.db-cache-clean-wait | How long, in milliseconds, database-level cache will keep entries after flushing them.  This option is only useful on distributed storage backends that are capable of acknowledging writes without necessarily making them immediately visible. | Integer | 50 | MASKABLE |
| cache.db-cache-size | Size of JanusGraph's database level cache.  Values between 0 and 1 are interpreted as a percentage of VM heap, while larger values are interpreted as an absolute size in bytes. | Double | 0.3 | MASKABLE |
| cache.db-cache-time | Default expiration time, in milliseconds, for entries in the database-level cache. Entries are evicted when they reach this age even if the cache has room to spare. Set to 0 to disable expiration (cache entries live forever or until memory pressure triggers eviction when set to 0). | Long | 10000 | MASKABLE |
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
| graph.allow-custom-vid-types | Whether non long-type vertex ids are allowed. set-vertex-id must be enabled in order to use this functionality. Currently, only string-type is supported. This does not prevent users from using custom ids with long type. If your storage backend does not support unordered scan, then some scan operations will be disabled. You cannot use this feature with Berkeley DB. EXPERT FEATURE - USE WITH GREAT CARE. | Boolean | false | GLOBAL_OFFLINE |
| graph.allow-stale-config | Whether to allow the local and storage-backend-hosted copies of the configuration to contain conflicting values for options with any of the following types: FIXED, GLOBAL_OFFLINE, GLOBAL.  These types are managed globally through the storage backend and cannot be overridden by changing the local configuration.  This type of conflict usually indicates misconfiguration.  When this option is true, JanusGraph will log these option conflicts, but continue normal operation using the storage-backend-hosted value for each conflicted option.  When this option is false, JanusGraph will log these option conflicts, but then it will throw an exception, refusing to start. | Boolean | true | MASKABLE |
| graph.allow-upgrade | Setting this to true will allow certain fixed values to be updated such as storage-version. This should only be used for upgrading. | Boolean | false | MASKABLE |
| graph.assign-timestamp | Whether to use JanusGraph generated client-side timestamp in mutations if the backend supports it. When enabled, JanusGraph assigns one timestamp to all insertions and another slightly earlier timestamp to all deletions in the same batch. When this is disabled, mutation behavior depends on the backend. Some might use server-side timestamp (e.g. HBase) while others might use client-side timestamp generated by driver (CQL). | Boolean | true | LOCAL |
| graph.graphname | This config option is an optional configuration setting that you may supply when opening a graph. The String value you provide will be the name of your graph. If you use the ConfigurationManagement APIs, then you will be able to access your graph by this String representation using the ConfiguredGraphFactory APIs. | String | (no default value) | LOCAL |
| graph.replace-instance-if-exists | If a JanusGraph instance with the same instance identifier already exists, the usage of this configuration option results in the opening of this graph anyway. | Boolean | false | LOCAL |
| graph.set-vertex-id | Whether user provided vertex ids should be enabled and JanusGraph's automatic vertex id allocation be disabled. Useful when operating JanusGraph in concert with another storage system that assigns long ids but disables some of JanusGraph's advanced features which can lead to inconsistent data. For example, users must ensure the vertex ids are unique to avoid duplication. Must use `graph.getIDManager().toVertexId(long)` to convert your id first. Once this is enabled, you have to provide vertex id when creating new vertices. EXPERT FEATURE - USE WITH GREAT CARE. | Boolean | false | GLOBAL_OFFLINE |
| graph.storage-version | The version of JanusGraph storage schema with which this database was created. Automatically set on first start of graph. Should only ever be changed if upgrading to a new major release version of JanusGraph that contains schema changes | String | (no default value) | FIXED |
| graph.timestamps | The timestamp resolution to use when writing to storage and indices. Sets the time granularity for the entire graph cluster. To avoid potential inaccuracies, the configured time resolution should match those of the backend systems. Some JanusGraph storage backends declare a preferred timestamp resolution that reflects design constraints in the underlying service. When the backend provides a preferred default, and when this setting is not explicitly declared in the config file, the backend default is used and the general default associated with this setting is ignored.  An explicit declaration of this setting overrides both the general and backend-specific defaults. | TimestampProviders | MICRO | FIXED |
| graph.unique-instance-id | Unique identifier for this JanusGraph instance.  This must be unique among all instances concurrently accessing the same stores or indexes.  It's automatically generated by concatenating the hostname, process id, and a static (process-wide) counter. Leaving it unset is recommended. | String | (no default value) | LOCAL |
| graph.unique-instance-id-suffix | When this is set and unique-instance-id is not, this JanusGraph instance's unique identifier is generated by concatenating the hex encoded hostname to the provided number. | Short | (no default value) | LOCAL |
| graph.use-hostname-for-unique-instance-id | When this is set, this JanusGraph's unique instance identifier is set to the hostname. If unique-instance-id-suffix is also set, then the identifier is set to <hostname><suffix>. | Boolean | false | LOCAL |

### graph.script-eval
Configuration options for gremlin script engine.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| graph.script-eval.enabled | Whether to enable Gremlin script evaluation. If it is enabled, a gremlin script engine will be instantiated together with the JanusGraph instance, with which one can use `eval` method to evaluate a gremlin script in plain string format. This is usually only useful when JanusGraph is used as an embedded Java library. | Boolean | false | MASKABLE |
| graph.script-eval.engine | Full class name of script engine that implements `GremlinScriptEngine` interface. Following shorthands can be used: <br> - `GremlinLangScriptEngine` (A script engine that only accepts standard gremlin queries. Anything else including lambda function is not accepted. We recommend using this because it's generally safer, but it is not guaranteed that it has no security problem.)- `GremlinGroovyScriptEngine` (A script engine that accepts arbitrary groovy code. This can be dangerous and you should use it at your own risk. See https://tinkerpop.apache.org/docs/current/reference/#script-execution for potential security problems.) | String | GremlinLangScriptEngine | MASKABLE |

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
| index.[X].map-name | Whether to use the name of the property key as the field name in the index. It must be ensured, that the indexed property key names are valid field names. Renaming the property key will NOT rename the field and its the developers responsibility to avoid field collisions. | Boolean | true | GLOBAL |
| index.[X].max-result-set-size | Maximum number of results to return if no limit is specified. For index backends that support scrolling, it represents the number of results in each batch | Integer | 50 | MASKABLE |
| index.[X].port | The port on which to connect to index backend servers | Integer | (no default value) | MASKABLE |

### index.[X].bkd-circle-processor
Configuration for BKD circle processors which is used for BKD Geoshape mapping.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].bkd-circle-processor.class | Full class name of circle processor that implements `CircleProcessor` interface. The class is used for transformation of a Circle shape to another shape when BKD mapping is used. The provided implementation class should have either a public constructor which accepts configuration as a parameter (`org.janusgraph.diskstorage.configuration.Configuration`) or a public constructor with no parameters. Usually the transforming shape is a Polygon. <br>Following shorthands can be used: <br> - `noTransformation` Circle processor which is not transforming a circle, but instead keep the circle shape unchanged. This implementation may be useful in situations when the user wants to control circle transformation logic on ElasticSearch side instead of application side. For example, using <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-circle-processor.html">ElasticSearch Circle Processor</a> or any custom plugin. <br> - `fixedErrorDistance` Circle processor which transforms the provided Circle into Polygon, Box, or Point depending on the configuration provided in `index.bkd-circle-processor.fixed`. The processing logic is similar to <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-circle-processor.html">ElasticSearch Circle Processor</a> except for some edge cases when the Circle is transformed into Box or Point. <br> - `dynamicErrorDistance` Circle processor which calculates error distance dynamically depending on the circle radius and the specified multiplier value. The error distance calculation formula is `log(radius) * multiplier`. Configuration for this class can be provided via `index.bkd-circle-processor.dynamic`. The processing logic is similar to <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-circle-processor.html">ElasticSearch Circle Processor</a> except for some edge cases when the Circle is transformed into Box or Point. | String | dynamicErrorDistance | MASKABLE |

### index.[X].bkd-circle-processor.dynamic
Configuration for Elasticsearch dynamic circle processor which is used for BKD Geoshape mapping.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].bkd-circle-processor.dynamic.bounding-box-fallback | Allows to return bounding box for the circle which cannot be converted to proper shape with the specified error distance. In case `false` is set for this configuration an exception will be thrown whenever circle cannot be converted to another shape following error distance. | Boolean | true | MASKABLE |
| index.[X].bkd-circle-processor.dynamic.error-distance-multiplier | Multiplier variable for dynamic error distance calculation in the formula `log(radius) * multiplier`. Radius and error distance specified in meters. | Double | 2.0 | MASKABLE |

### index.[X].bkd-circle-processor.fixed
Configuration for Elasticsearch fixed circle processor which is used for BKD Geoshape mapping.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].bkd-circle-processor.fixed.bounding-box-fallback | Allows to return bounding box for the circle which cannot be converted to proper shape with the specified error distance. In case `false` is set for this configuration an exception will be thrown whenever circle cannot be converted to another shape following error distance. | Boolean | true | MASKABLE |
| index.[X].bkd-circle-processor.fixed.error-distance | The difference between the resulting inscribed distance from center to side and the circle’s radius. Specified in meters. | Double | 10.0 | MASKABLE |

### index.[X].elasticsearch
Elasticsearch index configuration


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.bulk-refresh | Elasticsearch bulk API refresh setting used to control when changes made by this request are made visible to search | String | false | MASKABLE |
| index.[X].elasticsearch.client-keep-alive | Set a keep-alive timeout (in milliseconds) | Long | (no default value) | GLOBAL_OFFLINE |
| index.[X].elasticsearch.connect-timeout | Sets the maximum connection timeout (in milliseconds). | Integer | 1000 | MASKABLE |
| index.[X].elasticsearch.enable_index_names_cache | Enables cache for generated index store names. It is recommended to always enable index store names cache unless you have more then 50000 indexes per index store. | Boolean | true | MASKABLE |
| index.[X].elasticsearch.health-request-timeout | When JanusGraph initializes its ES backend, JanusGraph waits up to this duration for the ES cluster health to reach at least yellow status.  This string should be formatted as a natural number followed by the lowercase letter "s", e.g. 3s or 60s. | String | 30s | MASKABLE |
| index.[X].elasticsearch.interface | Interface for connecting to Elasticsearch. TRANSPORT_CLIENT and NODE were previously supported, but now are required to migrate to REST_CLIENT. See the JanusGraph upgrade instructions for more details. | String | REST_CLIENT | MASKABLE |
| index.[X].elasticsearch.retry_on_conflict | Specify how many times should the operation be retried when a conflict occurs. | Integer | 0 | MASKABLE |
| index.[X].elasticsearch.scroll-keep-alive | How long (in seconds) elasticsearch should keep alive the scroll context. | Integer | 60 | GLOBAL_OFFLINE |
| index.[X].elasticsearch.setup-max-open-scroll-contexts | Whether JanusGraph should setup max_open_scroll_context to maximum value for the cluster or not. | Boolean | true | MASKABLE |
| index.[X].elasticsearch.socket-timeout | Sets the maximum socket timeout (in milliseconds). | Integer | 30000 | MASKABLE |
| index.[X].elasticsearch.use-all-field | Whether JanusGraph should add an "all" field mapping. When enabled field mappings will include a "copy_to" parameter referencing the "all" field. This is supported since Elasticsearch 6.x  and is required when using wildcard fields starting in Elasticsearch 6.x. | Boolean | true | GLOBAL_OFFLINE |
| index.[X].elasticsearch.use-mapping-for-es7 | Mapping types are deprecated in ElasticSearch 7 and JanusGraph will not use mapping types by default for ElasticSearch 7 but if you want to preserve mapping types, you can setup this parameter to true. If you are updating ElasticSearch from 6 to 7 and you don't want to reindex your indexes, you may setup this parameter to true but we do recommend to reindex your indexes and don't use this parameter. | Boolean | false | MASKABLE |

### index.[X].elasticsearch.create
Settings related to index creation


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.create.allow-mapping-update | Whether JanusGraph should allow a mapping update when registering an index. Only applicable when use-external-mappings is true. | Boolean | false | MASKABLE |
| index.[X].elasticsearch.create.sleep | How long to sleep, in milliseconds, between the successful completion of a (blocking) index creation request and the first use of that index.  This only applies when creating an index in ES, which typically only happens the first time JanusGraph is started on top of ES. If the index JanusGraph is configured to use already exists, then this setting has no effect. | Long | 200 | MASKABLE |
| index.[X].elasticsearch.create.use-external-mappings | Whether JanusGraph should make use of an external mapping when registering an index. | Boolean | false | MASKABLE |

### index.[X].elasticsearch.create.ext
Overrides for arbitrary settings applied at index creation.
See [Elasticsearch](../index-backend/elasticsearch.md#index-creation-options), The full list of possible setting is available at [Elasticsearch index settings](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#index-modules-settings).


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.create.ext.number_of_replicas | The number of replicas each primary shard has | Integer | 1 | MASKABLE |
| index.[X].elasticsearch.create.ext.number_of_shards | The number of primary shards that an index should have.Default value is 5 on ES 6 and 1 on ES 7 | Integer | (no default value) | MASKABLE |

### index.[X].elasticsearch.http.auth
Configuration options for HTTP(S) authentication.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| index.[X].elasticsearch.http.auth.type | Authentication type to be used for HTTP(S) access. Available options are `NONE`, `BASIC` and `CUSTOM`. | String | NONE | LOCAL |

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
| index.[X].solr.dyn-fields | Whether to use dynamic fields (which appends the data type to the field name). If dynamic fields is disabled, the user must map field names and define them explicitly in the schema. | Boolean | true | GLOBAL_OFFLINE |
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
| log.[X].backend | Define the log backend to use. A reserved shortcut `default` can be used to use graph's storage backend to manage logs. A custom log implementation can be specified by providing full class path which implements `org.janusgraph.diskstorage.log.LogManager` and accepts a single parameter `org.janusgraph.diskstorage.configuration.Configuration` in the public constructor. | String | default | GLOBAL_OFFLINE |
| log.[X].fixed-partition | Whether all log entries are written to one fixed partition even if the backend store is partitioned.This can cause imbalanced loads and should only be used on low volume logs | Boolean | false | GLOBAL_OFFLINE |
| log.[X].key-consistent | Whether to require consistency for log reading and writing messages to the storage backend | Boolean | false | MASKABLE |
| log.[X].max-partitions | The maximum number of partitions to use for logging. Setting up this many actual or virtual partitions. Must be bigger than 0and a power of 2. | Integer | (no default value) | FIXED |
| log.[X].max-read-time | Maximum time in ms to try reading log messages from the backend before failing. | Duration | 4000 ms | MASKABLE |
| log.[X].max-write-time | Maximum time in ms to try persisting log messages against the backend before failing. | Duration | 10000 ms | MASKABLE |
| log.[X].num-buckets | The number of buckets to split log entries into for load balancing | Integer | 1 | GLOBAL_OFFLINE |
| log.[X].read-batch-size | Maximum number of log messages to read at a time for logging implementations that read messages in batches | Integer | 1024 | MASKABLE |
| log.[X].read-interval | Time in ms between message readings from the backend for this logging implementations that read message in batch | Duration | 5000 ms | MASKABLE |
| log.[X].read-lag-time | Maximum time in ms that it may take for reads to appear in the backend. If a write does not become visible in the storage backend in this amount of time, a log reader might miss the message. | Duration | 500 ms | MASKABLE |
| log.[X].read-threads | Number of threads to be used in reading and processing log messages | Integer | 1 | MASKABLE |
| log.[X].send-batch-size | Maximum number of log messages to batch up for sending for logging implementations that support batch sending | Integer | 256 | MASKABLE |
| log.[X].send-delay | Maximum time in ms that messages can be buffered locally before sending in batch | Duration | 1000 ms | MASKABLE |
| log.[X].ttl | Sets a TTL on all log entries, meaning that all entries added to this log expire after the configured amount of time. Requires that the log implementation supports TTL. | Duration | (no default value) | GLOBAL |

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
| query.fast-property | Whether to pre-fetch all properties on first singular vertex property access. This can eliminate backend calls on subsequent property access for the same vertex at the expense of retrieving all properties at once. This can be expensive for vertices with many properties. <br>This setting is applicable to direct vertex properties access (like `vertex.properties("foo")` but not to `vertex.properties("foo","bar")` because the latter case is not a singular property access). <br>This setting is not applicable to the next Gremlin steps: `valueMap`, `propertyMap`, `elementMap`, `properties`, `values` (configuration option `query.batch.properties-mode` should be used to configure their behavior).<br>When `true` this setting overwrites `query.batch.has-step-mode` to `all_properties` unless `none` mode is used. | Boolean | true | MASKABLE |
| query.force-index | Whether JanusGraph should throw an exception if a graph query cannot be answered using an index. Doing so limits the functionality of JanusGraph's graph queries but ensures that slow graph queries are avoided on large graphs. Recommended for production use of JanusGraph. | Boolean | false | MASKABLE |
| query.hard-max-limit | If smart-limit is disabled and no limit is given in the query, query optimizer adds a limit in light of possibly large result sets. It works in the same way as smart-limit except that hard-max-limit is usually a large number. Default value is Integer.MAX_VALUE which effectively disables this behavior. This option does not take effect when smart-limit is enabled. | Integer | 2147483647 | MASKABLE |
| query.ignore-unknown-index-key | Whether to ignore undefined types encountered in user-provided index queries | Boolean | false | MASKABLE |
| query.index-select-strategy | Name of the index selection strategy or full class name. Following shorthands can be used: <br>- `brute-force` (Try all combinations of index candidates and pick up optimal one)<br>- `approximate` (Use greedy algorithm to pick up approximately optimal index candidate)<br>- `threshold-based` (Use index-select-threshold to pick up either `approximate` or `threshold-based` strategy on runtime) | String | threshold-based | MASKABLE |
| query.index-select-threshold | Threshold of deciding whether to use brute force enumeration algorithm or fast approximation algorithm for selecting suitable indexes. Selecting optimal indexes for a query is a NP-complete set cover problem. When number of suitable index candidates is no larger than threshold, JanusGraph uses brute force search with exponential time complexity to ensure the best combination of indexes is selected. Only effective when `threshold-based` index select strategy is chosen. | Integer | 10 | MASKABLE |
| query.optimizer-backend-access | Whether the optimizer should be allowed to fire backend queries during the optimization phase. Allowing these will give the optimizer a chance to find more efficient execution plan but also increase the optimization overhead. | Boolean | true | MASKABLE |
| query.smart-limit | Whether the query optimizer should try to guess a smart limit for the query to ensure responsiveness in light of possibly large result sets. Those will be loaded incrementally if this option is enabled. | Boolean | false | MASKABLE |

### query.batch
Configuration options to configure batch queries optimization behavior


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| query.batch.enabled | Whether traversal queries should be batched when executed against the storage backend. This can lead to significant performance improvement if there is a non-trivial latency to the backend. If `false` then all other configuration options under `query.batch` namespace are ignored. | Boolean | true | MASKABLE |
| query.batch.has-step-mode | Properties pre-fetching mode for `has` step. Used only when `query.batch.enabled` is `true`.<br>Supported modes:<br>- `all_properties` - Pre-fetch all vertex properties on any property access (fetches all vertex properties in a single slice query)<br>- `required_properties_only` - Pre-fetch necessary vertex properties for the whole chain of foldable `has` steps (uses a separate slice query per each required property)<br>- `required_and_next_properties` - Prefetch the same properties as with `required_properties_only` mode, but also prefetch<br>properties which may be needed in the next properties access step like `values`, `properties,` `valueMap`, `elementMap`, or `propertyMap`.<br>In case the next step is not one of those properties access steps then this mode behaves same as `required_properties_only`.<br>In case the next step is one of the properties access steps with limited scope of properties, those properties will be<br>pre-fetched together in the same multi-query.<br>In case the next step is one of the properties access steps with unspecified scope of property keys then this mode<br>behaves same as `all_properties`.<br>- `required_and_next_properties_or_all` - Prefetch the same properties as with `required_and_next_properties`, but in case the next step is not<br>`values`, `properties,` `valueMap`, `elementMap`, or `propertyMap` then acts like `all_properties`.<br>- `none` - Skips `has` step batch properties pre-fetch optimization.<br> | String | required_and_next_properties | MASKABLE |
| query.batch.label-step-mode | Labels pre-fetching mode for `label()` step. Used only when `query.batch.enabled` is `true`.<br>Supported modes:<br>- `all` - Pre-fetch labels for all vertices in a batch.<br>- `none` - Skips vertex labels pre-fetching optimization.<br> | String | all | MASKABLE |
| query.batch.limited | Configure a maximum batch size for queries against the storage backend. This can be used to ensure responsiveness if batches tend to grow very large. The used batch size is equivalent to the barrier size of a preceding `barrier()` step. If a step has no preceding `barrier()`, the default barrier of TinkerPop will be inserted. This option only takes effect if `query.batch.enabled` is `true`. | Boolean | true | MASKABLE |
| query.batch.limited-size | Default batch size (barrier() step size) for queries. This size is applied only for cases where `LazyBarrierStrategy` strategy didn't apply `barrier` step and where user didn't apply barrier step either. This option is used only when `query.batch.limited` is `true`. Notice, value `2147483647` is considered to be unlimited. | Integer | 2500 | MASKABLE |
| query.batch.properties-mode | Properties pre-fetching mode for `values`, `properties`, `valueMap`, `propertyMap`, `elementMap` steps. Used only when `query.batch.enabled` is `true`.<br>Supported modes:<br>- `all_properties` - Pre-fetch all vertex properties on non-singular property access (fetches all vertex properties in a single slice query). On single property access this mode behaves the same as `required_properties_only` mode.<br>- `required_properties_only` - Pre-fetch necessary vertex properties only (uses a separate slice query per each required property)<br>- `none` - Skips vertex properties pre-fetching optimization.<br> | String | required_properties_only | MASKABLE |
| query.batch.repeat-step-mode | Batch mode for `repeat` step. Used only when query.batch.enabled is `true`.<br>These modes are controlling how the child steps with batch support are behaving if they are placed to the start of the `repeat`, `emit`, or `until` traversals.<br>Supported modes:<br>- `closest_repeat_parent` - Child start steps are receiving vertices for batching from the closest `repeat` step parent only.<br>- `all_repeat_parents` - Child start steps are receiving vertices for batching from all `repeat` step parents.<br>- `starts_only_of_all_repeat_parents` - Child start steps are receiving vertices for batching from the closest `repeat` step parent (both for the parent start and for next iterations) and also from all `repeat` step parents for the parent start. | String | all_repeat_parents | MASKABLE |

### schema
Schema related configuration options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| schema.constraints | Configures the schema constraints to be used by this graph. If config 'schema.constraints' is set to 'true' and 'schema.default' is set to 'none', then an 'IllegalArgumentException' is thrown for schema constraint violations. If 'schema.constraints' is set to 'true' and 'schema.default' is not set 'none', schema constraints are automatically created as described in the config option 'schema.default'. If 'schema.constraints' is set to 'false' which is the default, then no schema constraints are applied. | Boolean | false | GLOBAL_OFFLINE |
| schema.default | Configures the DefaultSchemaMaker to be used by this graph. Either one of the following shorthands can be used: <br> - `default` (a blueprints compatible schema maker with MULTI edge labels and SINGLE property keys),<br> - `tp3` (same as default, but has LIST property keys),<br> - `none` (automatic schema creation is disabled)<br> - `ignore-prop` (same as none, but simply ignore unknown properties rather than throw exceptions)<br> - or to the full package and classname of a custom/third-party implementing the interface `org.janusgraph.core.schema.DefaultSchemaMaker` | String | default | MASKABLE |
| schema.logging | Controls whether logging is enabled for schema makers. This only takes effect if you set `schema.default` to `default` or `ignore-prop`. For `default` schema maker, warning messages will be logged before schema types are created automatically. For `ignore-prop` schema maker, warning messages will be logged before unknown properties are ignored. | Boolean | false | MASKABLE |

### storage
Configuration options for the storage backend.  Some options are applicable only for certain backends.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.backend | The primary persistence provider used by JanusGraph.  This is required.  It should be set one of JanusGraph's built-in shorthand names for its standard storage backends (shorthands: berkeleyje, cql, hbase, inmemory, scylla) or to the full package and classname of a custom/third-party StoreManager implementation. | String | (no default value) | LOCAL |
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
| storage.read-time | Maximum time (in ms) to wait for a backend read operation to complete successfully. If a backend read operation fails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted.  | Duration | 10000 ms | MASKABLE |
| storage.root | Storage root directory for those storage backends that require local storage. If you do not supply storage.directory and you do supply graph.graphname, then your data will be stored in the directory equivalent to <STORAGE_ROOT>/<GRAPH_NAME>. | String | (no default value) | LOCAL |
| storage.setup-wait | Time in milliseconds for backend manager to wait for the storage backends to become available when JanusGraph is run in server mode | Duration | 60000 ms | MASKABLE |
| storage.transactions | Enables transactions on storage backends that support them | Boolean | true | MASKABLE |
| storage.username | Username to authenticate against backend | String | (no default value) | LOCAL |
| storage.write-time | Maximum time (in ms) to wait for a backend write operation to complete successfully. If a backend write operation fails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted.  | Duration | 100000 ms | MASKABLE |

### storage.berkeleyje
BerkeleyDB JE configuration options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.berkeleyje.cache-mode | Modes that can be specified for control over caching of records in the JE in-memory cache | String | DEFAULT | MASKABLE |
| storage.berkeleyje.cache-percentage | Percentage of JVM heap reserved for BerkeleyJE's cache | Integer | 65 | MASKABLE |
| storage.berkeleyje.isolation-level | The isolation level used by transactions | String | REPEATABLE_READ | MASKABLE |
| storage.berkeleyje.lock-mode | The BDB record lock mode used for read operations | String | LockMode.DEFAULT | MASKABLE |
| storage.berkeleyje.shared-cache | If true, the shared cache is used for all graph instances | Boolean | true | MASKABLE |

### storage.cql
CQL storage backend options


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.atomic-batch-mutate | True to use Cassandra atomic batch mutation, false to use non-atomic batches | Boolean | false | MASKABLE |
| storage.cql.back-pressure-class | The implementation of `QueryBackPressure` to use. The full name of the class which extends `QueryBackPressure` which has either a public constructor with `Configuration janusGraphConfiguration` and `Integer backPressureLimit` arguments (preferred constructor) or a public constructor with `Configuration janusGraphConfiguration` argument (second preferred constructor) or a public parameterless constructor. Other accepted options are:<br> `semaphore` - fair semaphore based back pressure implementation of `back-pressure-limit` limit size (preferred implementation);<br> `semaphoreReleaseProtected` - fair semaphore based back pressure implementation of `back-pressure-limit` limit size with protected releasing logic (meant to be used for testing);<br> `passAll` - turned off back pressure (it is recommended to tune CQL driver for the ongoing workload when this implementation is used); | String | semaphore | MASKABLE |
| storage.cql.back-pressure-limit | The maximum number of concurrent requests which are allowed to be processed by CQL driver. If no value is provided or the value is set to `0` then the value will be calculated based on CQL driver session provided parameters by using formula [advanced.connection.max-requests-per-connection * advanced.connection.pool.local.size * available_nodes_amount]. It's not recommended to use any value which is above this limit because it may result in CQL driver overload but it's suggested to have a lower value to keep the driver healthy under pressure. In situations when remote nodes connections are in use then the bigger value might be relevant as well to improve parallelism. | Integer | (no default value) | MASKABLE |
| storage.cql.batch-statement-size | The number of statements in each batch | Integer | 20 | MASKABLE |
| storage.cql.compaction-strategy-class | The compaction strategy to use for JanusGraph tables | String | (no default value) | FIXED |
| storage.cql.compaction-strategy-options | Compaction strategy options.  This list is interpreted as a map.  It must have an even number of elements in [key,val,key,val,...] form. | String[] | (no default value) | FIXED |
| storage.cql.compression | Whether the storage backend should use compression when storing the data | Boolean | true | FIXED |
| storage.cql.compression-block-size | The size of the compression blocks in kilobytes | Integer | 64 | FIXED |
| storage.cql.compression-type | The sstable_compression value JanusGraph uses when creating column families. This accepts any value allowed by Cassandra's sstable_compression option. Leave this unset to disable sstable_compression on JanusGraph-created CFs. | String | LZ4Compressor | MASKABLE |
| storage.cql.gc-grace-seconds | The number of seconds before tombstones (deletion markers) are eligible for garbage-collection. | Integer | (no default value) | FIXED |
| storage.cql.heartbeat-interval | The connection heartbeat interval in milliseconds. | Long | (no default value) | MASKABLE |
| storage.cql.heartbeat-timeout | How long the driver waits for the response (in milliseconds) to a heartbeat. | Long | (no default value) | MASKABLE |
| storage.cql.keyspace | The name of JanusGraph's keyspace.  It will be created if it does not exist. | String | janusgraph | LOCAL |
| storage.cql.local-datacenter | The name of the local or closest Cassandra datacenter. This value will be passed into CqlSessionBuilder.withLocalDatacenter. | String | datacenter1 | MASKABLE |
| storage.cql.local-max-connections-per-host | The maximum number of connections that can be created per host for local datacenter | Integer | 1 | MASKABLE |
| storage.cql.max-requests-per-connection | The maximum number of requests that can be executed concurrently on a connection. | Integer | 1024 | MASKABLE |
| storage.cql.metadata-schema-enabled | Whether schema metadata is enabled. | Boolean | (no default value) | MASKABLE |
| storage.cql.metadata-token-map-enabled | Whether token metadata is enabled. If disabled, partitioner-name must be provided. | Boolean | (no default value) | MASKABLE |
| storage.cql.only-use-local-consistency-for-system-operations | True to prevent any system queries from using QUORUM consistency and always use LOCAL_QUORUM instead | Boolean | false | MASKABLE |
| storage.cql.partitioner-name | The name of Cassandra cluster's partitioner. It will be retrieved by client if not provided. If provided, it must match the cluster's partitioner name. It can be the full class name such as `org.apache.cassandra.dht.ByteOrderedPartitioner` or the simple name such as `ByteOrderedPartitioner` | String | (no default value) | MASKABLE |
| storage.cql.protocol-version | The protocol version used to connect to the Cassandra database.  If no value is supplied then the driver will negotiate with the server. | Integer | 0 | LOCAL |
| storage.cql.read-consistency-level | The consistency level of read operations against Cassandra | String | QUORUM | MASKABLE |
| storage.cql.remote-max-connections-per-host | The maximum number of connections that can be created per host for remote datacenter | Integer | 1 | MASKABLE |
| storage.cql.replication-factor | The number of data replicas (including the original copy) that should be kept. This options is used when storage.cql.replication-strategy-class is set to SimpleStrategy | Integer | 1 | GLOBAL_OFFLINE |
| storage.cql.replication-strategy-class | The replication strategy to use for JanusGraph keyspace. Available strategies: SimpleStrategy,NetworkTopologyStrategy. | String | SimpleStrategy | FIXED |
| storage.cql.replication-strategy-options | Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a map.  It must have an even number of elements in [key,val,key,val,...] form. This options is used when storage.cql.replication-strategy-class is set to NetworkTopologyStrategy. `replication_factor` can be used to specify a replication factor. | String[] | (no default value) | FIXED |
| storage.cql.request-timeout | Timeout for CQL requests in milliseconds. See DataStax Java Driver option `basic.request.timeout` for more information. | Long | 12000 | MASKABLE |
| storage.cql.session-leak-threshold | The maximum number of live sessions that are allowed to coexist in a given VM until the warning starts to log for every new session. If the value is less than or equal to 0, the feature is disabled: no warning will be issued. See DataStax Java Driver option `advanced.session-leak.threshold` for more information. | Integer | (no default value) | MASKABLE |
| storage.cql.session-name | Default name for the Cassandra session | String | JanusGraph Session | MASKABLE |
| storage.cql.speculative-retry | The speculative retry policy. One of: NONE, ALWAYS, <X>percentile, <N>ms. | String | (no default value) | FIXED |
| storage.cql.ttl-enabled | Whether TTL should be enabled or not. Must be turned off if the storage does not support TTL. Amazon Keyspace, for example, does not support TTL by default unless otherwise enabled. | Boolean | true | LOCAL |
| storage.cql.use-external-locking | True to prevent JanusGraph from using its own locking mechanism. Setting this to true eliminates redundant checks when using an external locking mechanism outside of JanusGraph. Be aware that when use-external-locking is set to true, that failure to employ a locking algorithm which locks all columns that participate in a transaction upfront and unlocks them when the transaction ends, will result in a 'read uncommitted' transaction isolation level guarantee. If set to true without an appropriate external locking mechanism in place side effects such as dirty/non-repeatable/phantom reads should be expected. | Boolean | false | MASKABLE |
| storage.cql.write-consistency-level | The consistency level of write operations against Cassandra | String | QUORUM | MASKABLE |

### storage.cql.executor-service
Configuration options for CQL executor service which is used to process deserialization of CQL queries.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.executor-service.class | The implementation of `ExecutorService` to use. The full name of the class which extends `ExecutorService` which has either a public constructor with `ExecutorServiceConfiguration` argument (preferred constructor) or a public parameterless constructor. Other accepted options are: `fixed` - fixed thread pool of size `core-pool-size`; `cached` - cached thread pool; | String | fixed | LOCAL |
| storage.cql.executor-service.core-pool-size | Core pool size for executor service. May be ignored if custom executor service is used (depending on the implementation of the executor service).If not set or set to -1 the core pool size will be equal to number of processors multiplied by 2. | Integer | (no default value) | LOCAL |
| storage.cql.executor-service.keep-alive-time | Keep alive time in milliseconds for executor service. When the number of threads is greater than the `core-pool-size`, this is the maximum time that excess idle threads will wait for new tasks before terminating. Ignored for `fixed` executor service and may be ignored if custom executor service is used (depending on the implementation of the executor service). | Long | 60000 | LOCAL |
| storage.cql.executor-service.max-pool-size | Maximum pool size for executor service. Ignored for `fixed` and `cached` executor services. May be ignored if custom executor service is used (depending on the implementation of the executor service). | Integer | 2147483647 | LOCAL |
| storage.cql.executor-service.max-shutdown-wait-time | Max shutdown wait time in milliseconds for executor service threads to be finished during shutdown. After this time threads will be interrupted (signalled with interrupt) without any additional wait time. | Long | 60000 | LOCAL |

### storage.cql.grouping
Configuration options for controlling CQL queries grouping


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.grouping.keys-allowed | If `true` this allows multiple partition keys to be grouped together into a single CQL query via `IN` operator based on the keys grouping strategy provided (usually grouping is done by same token-ranges or same replica sets, but may also involve shard ids for custom implementations).<br>Notice, that any CQL query grouped with more than 1 key will require to return a row key for any column fetched.<br>This option is useful when less amount of CQL queries is desired to be sent for read requests in expense of fetching more data (partition key per each fetched value).<br>Notice, different storage backends may have different way of executing multi-partition `IN` queries (including, but not limited to how the checksum queries are sent for different consistency levels, processing node CPU usage, disk access pattern, etc.). Thus, a proper benchmarking is needed to determine if keys grouping is useful or not per case by case scenario.<br>This option can be enabled only for storage backends which support `PER PARTITION LIMIT`. As such, this feature can't be used with Amazon Keyspaces because it doesn't support `PER PARTITION LIMIT`.<br>If this option is `false` then each partition key will be executed in a separate asynchronous CQL query even when multiple keys from the same token range are queried.<br>Notice, the default grouping strategy does not take shards into account. Thus, this might be inefficient with ScyllaDB storage backend. ScyllaDB specific keys grouping strategy should be implemented after the resolution of the [ticket #232](https://github.com/scylladb/java-driver/issues/232). | Boolean | false | MASKABLE |
| storage.cql.grouping.keys-class | Full class path of the keys grouping execution strategy. The class should implement `org.janusgraph.diskstorage.cql.strategy.GroupedExecutionStrategy` interface and have a public constructor with two arguments `org.janusgraph.diskstorage.configuration.Configuration` and `org.janusgraph.diskstorage.cql.CQLStoreManager`.<br>Shortcuts available:<br>- `tokenRangeAware` - groups partition keys which belong to the same token range. Notice, this strategy does not take shards into account. Thus, this might be inefficient with ScyllaDB storage backend.<br>- `replicasAware` - groups partition keys which belong to the same replica sets (same nodes). Notice, this strategy does not take shards into account. Thus, this might be inefficient with ScyllaDB storage backend.<br><br>Usually `tokenRangeAware` grouping strategy provides more smaller groups where each group contain keys which are stored close to each other on a disk and may cause less disk seeks in some cases. However `replicasAware` grouping strategy groups keys per replica set which usually means fewer bigger groups to be used (i.e. less CQL requests).<br>This option takes effect only when `storage.cql.grouping.keys-allowed` is `true`. | String | replicasAware | MASKABLE |
| storage.cql.grouping.keys-limit | Maximum amount of the keys which can be grouped together into a single CQL query. If more keys are queried, they are going to be grouped into separate CQL queries.<br>Notice, for ScyllaDB this option should not exceed the maximum number of distinct clustering key restrictions per query which can be changed by ScyllaDB configuration option `max-partition-key-restrictions-per-query` (https://enterprise.docs.scylladb.com/branch-2022.2/faq.html#how-can-i-change-the-maximum-number-of-in-restrictions). For AstraDB this limit is set to 20 and usually it's fixed. However, you can ask customer support for a possibility to change the default threshold to your desired configuration via `partition_keys_in_select_failure_threshold` and `in_select_cartesian_product_failure_threshold` threshold configurations (https://docs.datastax.com/en/astra-serverless/docs/plan/planning.html#_cassandra_yaml).<br>Ensure that your storage backend allows more IN selectors than the one set via this configuration.<br>This option takes effect only when `storage.cql.grouping.keys-allowed` is `true`. | Integer | 20 | MASKABLE |
| storage.cql.grouping.keys-min | Minimum amount of keys to consider for grouping. Grouping will be skipped for any multi-key query which has less than this amount of keys (i.e. a separate CQL query will be executed for each key in such case).<br>Usually this configuration should always be set to `2`. It is useful to increase the value only in cases when queries with more keys should not be grouped, but be performed separately to increase parallelism in expense of the network overhead.<br>This option takes effect only when `storage.cql.grouping.keys-allowed` is `true`. | Integer | 2 | MASKABLE |
| storage.cql.grouping.slice-allowed | If `true` this allows multiple Slice queries which are allowed to be performed as non-range queries (i.e. direct equality operation) to be grouped together into a single CQL query via `IN` operator. Notice, currently only operations to fetch properties with Cardinality.SINGLE are allowed to be performed as non-range queries (edges fetching or properties with Cardinality SET or LIST won't be grouped together).<br>If this option is `false` then each Slice query will be executed in a separate asynchronous CQL query even when grouping is allowed. | Boolean | true | MASKABLE |
| storage.cql.grouping.slice-limit | Maximum amount of grouped together slice queries into a single CQL query.<br>Notice, for ScyllaDB this option should not exceed the maximum number of distinct clustering key restrictions per query which can be changed by ScyllaDB configuration option `max-partition-key-restrictions-per-query` (https://enterprise.docs.scylladb.com/branch-2022.2/faq.html#how-can-i-change-the-maximum-number-of-in-restrictions). For AstraDB this limit is set to 20 and usually it's fixed. However, you can ask customer support for a possibility to change the default threshold to your desired configuration via `partition_keys_in_select_failure_threshold` and `in_select_cartesian_product_failure_threshold` threshold configurations (https://docs.datastax.com/en/astra-serverless/docs/plan/planning.html#_cassandra_yaml).<br>Ensure that your storage backend allows more IN selectors than the one set via this configuration.<br>This option is used only when `storage.cql.grouping.slice-allowed` is `true`. | Integer | 20 | MASKABLE |

### storage.cql.internal
Advanced configuration of internal DataStax driver. Notice, all available configurations will be composed in the order. Non specified configurations will be skipped. By default only base configuration is enabled (which has the smallest priority. It means that you can overwrite any configuration used in base programmatic configuration by using any other configuration type). The configurations are composed in the next order (sorted by priority in descending order): `file-configuration`, `resource-configuration`, `string-configuration`, `url-configuration`, `base-programmatic-configuration` (which is controlled by `base-programmatic-configuration-enabled` property). Configurations with higher priority always overwrite configurations with lower priority. I.e. if the same configuration parameter is used in both `file-configuration` and `string-configuration` the configuration parameter from `file-configuration` will be used and configuration parameter from `string-configuration` will be ignored. See available configuration options and configurations structure here: https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/configuration/reference/


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.internal.base-programmatic-configuration-enabled | Whether to use main programmatic configuration provided by JanusGraph properties or not. We don't recommend to disable this property unless you want to disable usage of all storage.cql properties and use default configurations or other configurations. If programmatic configuration options miss some important configuration options you can provide those missing configurations with other configuration types which will be applied with programmatic configuration (see other configuration types in this section). For most use cases this option should always be `true`. JanusGraph behaviour might be unpredictable when using unspecified configuration options. | Boolean | true | MASKABLE |
| storage.cql.internal.file-configuration | Path to file with DataStax configuration. | String | (no default value) | LOCAL |
| storage.cql.internal.resource-configuration | Classpath resource with DataStax configuration. | String | (no default value) | MASKABLE |
| storage.cql.internal.string-configuration | String representing DataStax configuration. | String | (no default value) | MASKABLE |
| storage.cql.internal.url-configuration | Url where to get DataStax configuration. | String | (no default value) | MASKABLE |

### storage.cql.metrics
Configuration options for CQL metrics


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.metrics.cql-messages-delay-refresh-interval | The interval at which percentile data is refreshed in milliseconds for requests. Used if 'cql-messages' node metric is enabled. See DataStax driver configuration option `advanced.metrics.node.cql-messages.refresh-interval` | Long | (no default value) | LOCAL |
| storage.cql.metrics.cql-messages-highest-latency | The largest latency that we expect to record for requests in milliseconds. Used if 'cql-messages' node metric is enabled. See DataStax driver configuration option `advanced.metrics.node.cql-messages.highest-latency` | Long | (no default value) | LOCAL |
| storage.cql.metrics.cql-messages-significant-digits | The number of significant decimal digits to which internal structures will maintain value resolution and separation for requests. This must be between 0 and 5. Used if 'cql-messages' node metric is enabled. See DataStax driver configuration option `advanced.metrics.node.cql-messages.significant-digits` | Integer | (no default value) | LOCAL |
| storage.cql.metrics.cql-requests-highest-latency | The largest latency that we expect to record for requests in milliseconds. Used if 'cql-requests' session metric is enabled. See DataStax driver configuration option `advanced.metrics.session.cql-requests.highest-latency` | Long | (no default value) | LOCAL |
| storage.cql.metrics.cql-requests-refresh-interval | The interval at which percentile data is refreshed in milliseconds for requests. Used if 'cql-requests' session metric is enabled. See DataStax driver configuration option `advanced.metrics.session.cql-requests.refresh-interval` | Long | (no default value) | LOCAL |
| storage.cql.metrics.cql-requests-significant-digits | The number of significant decimal digits to which internal structures will maintain value resolution and separation for requests. This must be between 0 and 5. Used if 'cql-requests' session metric is enabled. See DataStax driver configuration option `advanced.metrics.session.cql-requests.significant-digits` | Integer | (no default value) | LOCAL |
| storage.cql.metrics.node-enabled | Comma separated list of enabled node metrics. Used only when basic metrics are enabled. Check DataStax Cassandra Driver 4 documentation for available metrics (example: pool.open-connections, pool.available-streams, bytes-sent). | String[] | (no default value) | LOCAL |
| storage.cql.metrics.node-expire-after | The time after which the node level metrics will be evicted in milliseconds. | Long | (no default value) | LOCAL |
| storage.cql.metrics.session-enabled | Comma separated list of enabled session metrics. Used only when basic metrics are enabled. Check DataStax Cassandra Driver 4 documentation for available metrics (example: bytes-sent, bytes-received, connected-nodes). | String[] | (no default value) | LOCAL |
| storage.cql.metrics.throttling-delay-highest-latency | The largest latency that we expect to record for throttling in milliseconds. Used if 'throttling.delay' session metric is enabled. See DataStax driver configuration option `advanced.metrics.session.throttling.delay.highest-latency` | Long | (no default value) | LOCAL |
| storage.cql.metrics.throttling-delay-refresh-interval | The interval at which percentile data is refreshed in milliseconds for throttling. Used if 'throttling.delay' session metric is enabled. See DataStax driver configuration option `advanced.metrics.session.throttling.delay.refresh-interval` | Long | (no default value) | LOCAL |
| storage.cql.metrics.throttling-delay-significant-digits | The number of significant decimal digits to which internal structures will maintain value resolution and separation for throttling. This must be between 0 and 5. Used if 'throttling.delay' session metric is enabled. See DataStax driver configuration option `advanced.metrics.session.throttling.delay.significant-digits` | Integer | (no default value) | LOCAL |

### storage.cql.netty
Configuration options related to the Netty event loop groups used internally by the CQL driver.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.netty.admin-size | The number of threads for the event loop group used for admin tasks not related to request I/O (handle cluster events, refresh metadata, schedule reconnections, etc.). If this is not set, the driver will use 2. | Integer | (no default value) | LOCAL |
| storage.cql.netty.io-size | The number of threads for the event loop group used for I/O operations (reading and writing to Cassandra nodes). If this is not set, the driver will use `Runtime.getRuntime().availableProcessors() * 2`. | Integer | (no default value) | LOCAL |
| storage.cql.netty.timer-tick-duration | The timer tick duration in milliseconds. This is how frequent the timer should wake up to check for timed-out tasks or speculative executions. See DataStax Java Driver option `advanced.netty.timer.tick-duration` for more information. | Long | (no default value) | LOCAL |
| storage.cql.netty.timer-ticks-per-wheel | Number of ticks in a Timer wheel. See DataStax Java Driver option `advanced.netty.timer.ticks-per-wheel` for more information. | Integer | (no default value) | LOCAL |

### storage.cql.request-tracker
Configuration options for CQL request tracker and builtin request logger


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.request-tracker.class | It is either a predefined DataStax driver value for a builtin request tracker or a full qualified class name which implements `com.datastax.oss.driver.internal.core.tracker.RequestTracker` interface. If no any value provided, the default DataStax request tracker is used, which is `NoopRequestTracker` which doesn't do anything. If `RequestLogger` value is provided, the DataStax [RequestLogger](https://docs.datastax.com/en/developer/java-driver/4.9/manual/core/request_tracker/#request-logger) is used. | String | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-error-enabled | Whether to log failed requests.Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Boolean | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-max-query-length | The maximum length of the query string in the log message. Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Integer | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-max-value-length | The maximum length for bound values in the log message. Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Integer | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-max-values | The maximum number of bound values to log. Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Integer | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-show-stack-traces | Whether to log stack traces for failed queries. Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Boolean | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-show-values | Whether to log bound values in addition to the query string. Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Boolean | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-slow-enabled | Whether to log `slow` requests.Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Boolean | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-slow-threshold | The threshold to classify a successful request as `slow`. In milliseconds. Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Long | (no default value) | LOCAL |
| storage.cql.request-tracker.logs-success-enabled | Whether to log successful requests. Can be used when `root.storage.cql.request-tracker.class` is set to `RequestLogger`. | Boolean | (no default value) | LOCAL |

### storage.cql.ssl
Configuration options for SSL


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.ssl.client-authentication-enabled | Enables use of a client key to authenticate with Cassandra | Boolean | false | LOCAL |
| storage.cql.ssl.enabled | Controls use of the SSL connection to Cassandra | Boolean | false | LOCAL |
| storage.cql.ssl.hostname_validation | Enable / disable SSL hostname validation. | Boolean | false | LOCAL |

### storage.cql.ssl.keystore
Configuration options for SSL Keystore.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.cql.ssl.keystore.keypassword | The password to access the key in SSL Keystore. | String |  | LOCAL |
| storage.cql.ssl.keystore.location | Marks the location of the SSL Keystore. | String |  | LOCAL |
| storage.cql.ssl.keystore.storepassword | The password to access the SSL Keystore. | String |  | LOCAL |

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
| storage.hbase.compression-algorithm | An HBase Compression.Algorithm enum string which will be applied to newly created column families. The compression algorithm must be installed and available on the HBase cluster.  JanusGraph cannot install and configure new compression algorithms on the HBase cluster by itself. | String | GZ | MASKABLE |
| storage.hbase.region-count | The number of initial regions set when creating JanusGraph's HBase table | Integer | (no default value) | MASKABLE |
| storage.hbase.regions-per-server | The number of regions per regionserver to set when creating JanusGraph's HBase table | Integer | (no default value) | MASKABLE |
| storage.hbase.short-cf-names | Whether to shorten the names of JanusGraph's column families to one-character mnemonics to conserve storage space | Boolean | true | FIXED |
| storage.hbase.skip-schema-check | Assume that JanusGraph's HBase table and column families already exist. When this is true, JanusGraph will not check for the existence of its table/CFs, nor will it attempt to create them under any circumstances.  This is useful when running JanusGraph without HBase admin privileges. | Boolean | false | MASKABLE |
| storage.hbase.snapshot-name | The name of an existing HBase snapshot to be used by HBaseSnapshotInputFormat | String | janusgraph-snapshot | LOCAL |
| storage.hbase.snapshot-restore-dir | The temporary directory to be used by HBaseSnapshotInputFormat to restore a snapshot. This directory should be on the same File System as the HBase root dir. | String | /tmp | LOCAL |
| storage.hbase.table | The name of the table JanusGraph will use.  When storage.hbase.skip-schema-check is false, JanusGraph will automatically create this table if it does not already exist. If this configuration option is not provided but graph.graphname is, the table will be set to that value. | String | janusgraph | LOCAL |

### storage.lock
Options for locking on eventually-consistent stores


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.lock.backend | Locker type to use | String | consistentkey | GLOBAL_OFFLINE |
| storage.lock.clean-expired | Whether to delete expired locks from the storage backend | Boolean | false | MASKABLE |
| storage.lock.expiry-time | Number of milliseconds after which a lock is considered to have expired. Lock applications that were not released are considered expired after this time and released. This value should be larger than the maximum time a transaction can take in order to guarantee that no correctly held applications are expired pre-maturely and as small as possible to avoid dead lock. | Duration | 300000 ms | GLOBAL_OFFLINE |
| storage.lock.local-mediator-group | This option determines the LocalLockMediator instance used for early detection of lock contention between concurrent JanusGraph graph instances within the same process which are connected to the same storage backend.  JanusGraph instances that have the same value for this variable will attempt to discover lock contention among themselves in memory before proceeding with the general-case distributed locking code.  JanusGraph generates an appropriate default value for this option at startup.  Overriding the default is generally only useful in testing. | String | (no default value) | LOCAL |
| storage.lock.retries | Number of times the system attempts to acquire a lock before giving up and throwing an exception | Integer | 3 | MASKABLE |
| storage.lock.wait-time | Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend. Also, the time waited at the end of all lock applications before verifying that the applications were successful. This value should be a small multiple of the average consistent write time. Although this value is maskable, it is highly recommended to use the same value across JanusGraph instances in production environments. | Duration | 100 ms | MASKABLE |

### storage.meta *
Meta data to include in storage backend retrievals


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.meta.[X].timestamps | Whether to include timestamps in retrieved entries for storage backends that automatically annotated entries with timestamps. If enabled, timestamp can be retrieved by `element.value(ImplicitKey.TIMESTAMP.name())` or equivalently, `element.value("~timestamp")`. | Boolean | false | GLOBAL |
| storage.meta.[X].ttl | Whether to include ttl in retrieved entries for storage backends that support storage and retrieval of cell level TTL. If enabled, ttl can be retrieved by `element.value(ImplicitKey.TTL.name())` or equivalently, `element.value("~ttl")`. | Boolean | false | GLOBAL |
| storage.meta.[X].visibility | Whether to include visibility in retrieved entries for storage backends that support cell level visibility. If enabled, visibility can be retrieved by `element.value(ImplicitKey.VISIBILITY.name())` or equivalently, `element.value("~visibility")`. | Boolean | true | GLOBAL |

### storage.parallel-backend-executor-service
Configuration options for executor service which is used for parallel requests when `storage.parallel-backend-ops` is enabled.


| Name | Description | Datatype | Default Value | Mutability |
| ---- | ---- | ---- | ---- | ---- |
| storage.parallel-backend-executor-service.class | The implementation of `ExecutorService` to use. The full name of the class which extends `ExecutorService` which has either a public constructor with `ExecutorServiceConfiguration` argument (preferred constructor) or a public parameterless constructor. Other accepted options are: `fixed` - fixed thread pool of size `core-pool-size`; `cached` - cached thread pool; | String | fixed | LOCAL |
| storage.parallel-backend-executor-service.core-pool-size | Core pool size for executor service. May be ignored if custom executor service is used (depending on the implementation of the executor service).If not set or set to -1 the core pool size will be equal to number of processors multiplied by 2. | Integer | (no default value) | LOCAL |
| storage.parallel-backend-executor-service.keep-alive-time | Keep alive time in milliseconds for executor service. When the number of threads is greater than the `core-pool-size`, this is the maximum time that excess idle threads will wait for new tasks before terminating. Ignored for `fixed` executor service and may be ignored if custom executor service is used (depending on the implementation of the executor service). | Long | 60000 | LOCAL |
| storage.parallel-backend-executor-service.max-pool-size | Maximum pool size for executor service. Ignored for `fixed` and `cached` executor services. May be ignored if custom executor service is used (depending on the implementation of the executor service). | Integer | 2147483647 | LOCAL |
| storage.parallel-backend-executor-service.max-shutdown-wait-time | Max shutdown wait time in milliseconds for executor service threads to be finished during shutdown. After this time threads will be interrupted (signalled with interrupt) without any additional wait time. | Long | 60000 | LOCAL |

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
