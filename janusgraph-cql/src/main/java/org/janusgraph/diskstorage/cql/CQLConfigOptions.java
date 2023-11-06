// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ExecutorServiceBuilder;
import org.janusgraph.diskstorage.cql.strategy.GroupedExecutionStrategyBuilder;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;
import org.janusgraph.diskstorage.util.backpressure.builder.QueryBackPressureBuilder;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration options for the CQL storage backend. These are managed under the 'cql' namespace in the configuration.
 */
@PreInitializeConfigOptions
public interface CQLConfigOptions {

    ConfigNamespace CQL_NS = new ConfigNamespace(
            GraphDatabaseConfiguration.STORAGE_NS,
            "cql",
            "CQL storage backend options");

    ConfigOption<String> KEYSPACE = new ConfigOption<>(
            CQL_NS,
            "keyspace",
            "The name of JanusGraph's keyspace.  It will be created if it does not exist.",
            ConfigOption.Type.LOCAL,
            "janusgraph");

    ConfigOption<Integer> PROTOCOL_VERSION = new ConfigOption<>(
            CQL_NS,
            "protocol-version",
            "The protocol version used to connect to the Cassandra database.  If no value is supplied then the driver will negotiate with the server.",
            ConfigOption.Type.LOCAL,
            0);

    ConfigOption<String> READ_CONSISTENCY = new ConfigOption<>(
            CQL_NS,
            "read-consistency-level",
            "The consistency level of read operations against Cassandra",
            ConfigOption.Type.MASKABLE,
            CQLStoreManager.CONSISTENCY_QUORUM);

    ConfigOption<String> WRITE_CONSISTENCY = new ConfigOption<>(
            CQL_NS,
            "write-consistency-level",
            "The consistency level of write operations against Cassandra",
            ConfigOption.Type.MASKABLE,
            CQLStoreManager.CONSISTENCY_QUORUM);

    ConfigOption<Integer> INIT_WAIT_TIME = new ConfigOption<>(
            CQL_NS,
            "init-wait-time",
            "Number of milliseconds to sleep after creating each keyspace/table, only needed if table creation " +
            "is asynchronous, e.g. Amazon Keyspace",
            ConfigOption.Type.LOCAL, Integer.class);

    ConfigOption<Boolean> ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS =
        new ConfigOption<>(CQL_NS, "only-use-local-consistency-for-system-operations",
            "True to prevent any system queries from using QUORUM consistency " +
                "and always use LOCAL_QUORUM instead",
            ConfigOption.Type.MASKABLE, false);

    ConfigOption<Boolean> USE_EXTERNAL_LOCKING = new ConfigOption<>(
            CQL_NS,
            "use-external-locking",
            "True to prevent JanusGraph from using its own locking mechanism. Setting this to true eliminates " +
            "redundant checks when using an external locking mechanism outside of JanusGraph. Be aware that " +
            "when use-external-locking is set to true, that failure to employ a locking algorithm which locks " +
            "all columns that participate in a transaction upfront and unlocks them when the transaction ends, " +
            "will result in a 'read uncommitted' transaction isolation level guarantee. If set to true without " +
            "an appropriate external locking mechanism in place side effects such as " +
            "dirty/non-repeatable/phantom reads should be expected.",
            ConfigOption.Type.MASKABLE,
            false);

    // The number of statements in a batch
    ConfigOption<Integer> BATCH_STATEMENT_SIZE = new ConfigOption<>(
            CQL_NS,
            "batch-statement-size",
            "The number of statements in each batch",
            ConfigOption.Type.MASKABLE,
            20);

    // Whether to use un-logged batches
    ConfigOption<Boolean> ATOMIC_BATCH_MUTATE = new ConfigOption<>(
            CQL_NS,
            "atomic-batch-mutate",
            "True to use Cassandra atomic batch mutation, false to use non-atomic batches",
            ConfigOption.Type.MASKABLE,
            false);

    // Replication

    String SIMPLE_REPLICATION_STRATEGY = "SimpleStrategy";
    String NETWORK_TOPOLOGY_REPLICATION_STRATEGY = "NetworkTopologyStrategy";

    ConfigOption<String> REPLICATION_STRATEGY = new ConfigOption<>(
        CQL_NS,
        "replication-strategy-class",
        "The replication strategy to use for JanusGraph keyspace. Available strategies: "+
            String.join(",",new String[]{SIMPLE_REPLICATION_STRATEGY, NETWORK_TOPOLOGY_REPLICATION_STRATEGY})+".",
        ConfigOption.Type.FIXED,
        SIMPLE_REPLICATION_STRATEGY);

    ConfigOption<Integer> REPLICATION_FACTOR = new ConfigOption<>(
            CQL_NS,
            "replication-factor",
        "The number of data replicas (including the original copy) that should be kept. This options is used when "
            +ConfigElement.getPath(REPLICATION_STRATEGY)+" is set to "+SIMPLE_REPLICATION_STRATEGY,
            ConfigOption.Type.GLOBAL_OFFLINE,
            1);

    ConfigOption<String[]> REPLICATION_OPTIONS = new ConfigOption<>(
            CQL_NS,
            "replication-strategy-options",
            "Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a " +
                "map.  It must have an even number of elements in [key,val,key,val,...] form. This options is used when "
                +ConfigElement.getPath(REPLICATION_STRATEGY)+" is set to "+NETWORK_TOPOLOGY_REPLICATION_STRATEGY+". " +
                "`replication_factor` can be used to specify a replication factor.",
            ConfigOption.Type.FIXED,
            String[].class);

    ConfigOption<String> COMPACTION_STRATEGY = new ConfigOption<>(
            CQL_NS,
            "compaction-strategy-class",
            "The compaction strategy to use for JanusGraph tables",
            ConfigOption.Type.FIXED,
            String.class);

    ConfigOption<String[]> COMPACTION_OPTIONS = new ConfigOption<>(
            CQL_NS,
            "compaction-strategy-options",
            "Compaction strategy options.  This list is interpreted as a " +
                    "map.  It must have an even number of elements in [key,val,key,val,...] form.",
            ConfigOption.Type.FIXED,
            String[].class);

    ConfigOption<String> SPECULATIVE_RETRY = new ConfigOption<>(
        CQL_NS,
        "speculative-retry",
        "The speculative retry policy. One of: NONE, ALWAYS, <X>percentile, <N>ms.",
        ConfigOption.Type.FIXED,
        String.class);

    ConfigOption<Integer> GC_GRACE_SECONDS = new ConfigOption<>(
        CQL_NS,
        "gc-grace-seconds",
        "The number of seconds before tombstones (deletion markers) are eligible for garbage-collection.",
        ConfigOption.Type.FIXED,
        Integer.class);

    // Compression
    ConfigOption<Boolean> CF_COMPRESSION = new ConfigOption<>(
            CQL_NS,
            "compression",
            "Whether the storage backend should use compression when storing the data",
            ConfigOption.Type.FIXED,
            true);

    ConfigOption<String> CF_COMPRESSION_TYPE = new ConfigOption<>(
            CQL_NS,
            "compression-type",
            "The sstable_compression value JanusGraph uses when creating column families. " +
                    "This accepts any value allowed by Cassandra's sstable_compression option. " +
                    "Leave this unset to disable sstable_compression on JanusGraph-created CFs.",
            ConfigOption.Type.MASKABLE,
            "LZ4Compressor");

    ConfigOption<Integer> CF_COMPRESSION_BLOCK_SIZE = new ConfigOption<>(
            CQL_NS,
            "compression-block-size",
            "The size of the compression blocks in kilobytes",
            ConfigOption.Type.FIXED,
            64);

    ConfigOption<Integer> LOCAL_MAX_CONNECTIONS_PER_HOST = new ConfigOption<>(
            CQL_NS,
            "local-max-connections-per-host",
            "The maximum number of connections that can be created per host for local datacenter",
            ConfigOption.Type.MASKABLE,
            1);

    ConfigOption<Integer> REMOTE_MAX_CONNECTIONS_PER_HOST = new ConfigOption<>(
            CQL_NS,
            "remote-max-connections-per-host",
            "The maximum number of connections that can be created per host for remote datacenter",
            ConfigOption.Type.MASKABLE,
            1);

    ConfigOption<Integer> MAX_REQUESTS_PER_CONNECTION = new ConfigOption<>(
            CQL_NS,
            "max-requests-per-connection",
            "The maximum number of requests that can be executed concurrently on a connection.",
            ConfigOption.Type.MASKABLE,
            1024);

    ConfigOption<Integer> BACK_PRESSURE_LIMIT = new ConfigOption<>(
        CQL_NS,
        "back-pressure-limit",
        "The maximum number of concurrent requests which are allowed to be processed by CQL driver. " +
            "If no value is provided or the value is set to `0` then the value will be calculated based on CQL driver " +
            "session provided parameters by using formula ["+DefaultDriverOption.CONNECTION_MAX_REQUESTS.getPath()+" * "
            +DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath()+" * available_nodes_amount]. " +
            "It's not recommended to use any value which is above this limit because it may result in CQL driver overload " +
            "but it's suggested to have a lower value to keep the driver healthy under pressure. In situations when remote " +
            "nodes connections are in use then the bigger value might be relevant as well to improve parallelism.",
        ConfigOption.Type.MASKABLE,
        Integer.class);

    ConfigOption<String> BACK_PRESSURE_CLASS = new ConfigOption<>(
        CQL_NS,
        "back-pressure-class",
        "The implementation of `QueryBackPressure` to use. " +
            "The full name of the class which extends `"+ QueryBackPressure.class.getSimpleName()+"` which has either " +
            "a public constructor with `"+ Configuration.class.getSimpleName()+" janusGraphConfiguration` and `"+
            Integer.class.getSimpleName()+" backPressureLimit` arguments (preferred constructor) or a public constructor with `"+
            Configuration.class.getSimpleName()+" janusGraphConfiguration` argument (second preferred constructor) or "+
            "a public parameterless constructor. Other accepted options are:<br> `"+
            QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS+"` - fair semaphore based back pressure implementation of `"
            +BACK_PRESSURE_LIMIT.getName()+"` limit size (preferred implementation);<br> `"
            +QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS+"` - fair semaphore based back pressure " +
            "implementation of `"+BACK_PRESSURE_LIMIT.getName()+"` limit size with protected releasing logic (meant to be used for testing);<br> `"
            +QueryBackPressureBuilder.PASS_ALL_QUERY_BACK_PRESSURE_CLASS+"` - turned off back pressure (it is recommended to tune " +
            "CQL driver for the ongoing workload when this implementation is used);",
        ConfigOption.Type.MASKABLE,
        String.class,
        QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS);

    ConfigOption<Long> HEARTBEAT_INTERVAL = new ConfigOption<>(
        CQL_NS,
        "heartbeat-interval",
        "The connection heartbeat interval in milliseconds.",
        ConfigOption.Type.MASKABLE,
        Long.class);

    ConfigOption<Long> HEARTBEAT_TIMEOUT = new ConfigOption<>(
        CQL_NS,
        "heartbeat-timeout",
        "How long the driver waits for the response (in milliseconds) to a heartbeat.",
        ConfigOption.Type.MASKABLE,
        Long.class);

    ConfigOption<Boolean> TTL_ENABLED = new ConfigOption<>(
        CQL_NS,
        "ttl-enabled",
        "Whether TTL should be enabled or not. Must be turned off if the storage does not support TTL. " +
            "Amazon Keyspace, for example, does not support TTL by default unless otherwise enabled.",
        ConfigOption.Type.LOCAL,
        Boolean.class,
        true);

    ConfigOption<String> PARTITIONER_NAME = new ConfigOption<>(
        CQL_NS,
        "partitioner-name",
        "The name of Cassandra cluster's partitioner. It will be retrieved by client if not provided. " +
            "If provided, it must match the cluster's partitioner name. It can be the full class name such as " +
            "`org.apache.cassandra.dht.ByteOrderedPartitioner` or the simple name such as `ByteOrderedPartitioner`",
        ConfigOption.Type.MASKABLE,
        String.class);

    ConfigOption<Boolean> METADATA_SCHEMA_ENABLED = new ConfigOption<>(
        CQL_NS,
        "metadata-schema-enabled",
        "Whether schema metadata is enabled.",
        ConfigOption.Type.MASKABLE,
        Boolean.class);

    ConfigOption<Boolean> METADATA_TOKEN_MAP_ENABLED = new ConfigOption<>(
        CQL_NS,
        "metadata-token-map-enabled",
        "Whether token metadata is enabled. If disabled, partitioner-name must be provided.",
        ConfigOption.Type.MASKABLE,
        Boolean.class);

    // SSL
    ConfigNamespace SSL_NS = new ConfigNamespace(
            CQL_NS,
            "ssl",
            "Configuration options for SSL");

    ConfigNamespace SSL_KEYSTORE_NS = new ConfigNamespace(
            SSL_NS,
            "keystore",
            "Configuration options for SSL Keystore.");

    ConfigNamespace SSL_TRUSTSTORE_NS = new ConfigNamespace(
            SSL_NS,
            "truststore",
            "Configuration options for SSL Truststore.");

    ConfigOption<Boolean> SSL_CLIENT_AUTHENTICATION_ENABLED = new ConfigOption<>(
            SSL_NS,
            "client-authentication-enabled",
            "Enables use of a client key to authenticate with Cassandra",
            ConfigOption.Type.LOCAL,
            false);

    ConfigOption<Boolean> SSL_ENABLED = new ConfigOption<>(
            SSL_NS,
            "enabled",
            "Controls use of the SSL connection to Cassandra",
            ConfigOption.Type.LOCAL,
            false);

    ConfigOption<Boolean> SSL_HOSTNAME_VALIDATION = new ConfigOption<>(
        SSL_NS,
        "hostname_validation",
        "Enable / disable SSL hostname validation.",
        ConfigOption.Type.LOCAL,
        false);

    ConfigOption<String> SSL_KEYSTORE_LOCATION = new ConfigOption<>(
            SSL_KEYSTORE_NS,
            "location",
            "Marks the location of the SSL Keystore.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<String> SSL_KEYSTORE_KEY_PASSWORD = new ConfigOption<>(
            SSL_KEYSTORE_NS,
            "keypassword",
            "The password to access the key in SSL Keystore.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<String> SSL_KEYSTORE_STORE_PASSWORD = new ConfigOption<>(
            SSL_KEYSTORE_NS,
            "storepassword",
            "The password to access the SSL Keystore.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<String> SSL_TRUSTSTORE_LOCATION = new ConfigOption<>(
            SSL_TRUSTSTORE_NS,
            "location",
            "Marks the location of the SSL Truststore.",
            ConfigOption.Type.LOCAL,
            "");

    ConfigOption<String> SSL_TRUSTSTORE_PASSWORD = new ConfigOption<>(
            SSL_TRUSTSTORE_NS,
            "password",
            "The password to access SSL Truststore.",
            ConfigOption.Type.LOCAL,
            "");

    // Other options
    ConfigOption<String> SESSION_NAME = new ConfigOption<>(
            CQL_NS,
            "session-name",
            "Default name for the Cassandra session",
            ConfigOption.Type.MASKABLE,
            "JanusGraph Session");

    ConfigOption<String> LOCAL_DATACENTER = new ConfigOption<>(
            CQL_NS,
            "local-datacenter",
            "The name of the local or closest Cassandra datacenter. " +
                "This value will be passed into CqlSessionBuilder.withLocalDatacenter.",
            /*
             * It's between either LOCAL or MASKABLE. MASKABLE could be useful for cases where all the JanusGraph instances are closest to
             * the same Cassandra DC.
             */
            ConfigOption.Type.MASKABLE,
            String.class,
        "datacenter1");

    // Netty

    ConfigNamespace NETTY = new ConfigNamespace(
        CQL_NS,
        "netty",
        "Configuration options related to the Netty event loop groups used internally by the CQL driver.");

    ConfigOption<Integer> NETTY_IO_SIZE = new ConfigOption<>(
        NETTY,
        "io-size",
        "The number of threads for the event loop group used for I/O operations " +
            "(reading and writing to Cassandra nodes). " +
            "If this is not set, the driver will use `Runtime.getRuntime().availableProcessors() * 2`.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Integer> NETTY_ADMIN_SIZE = new ConfigOption<>(
        NETTY,
        "admin-size",
        "The number of threads for the event loop group used for admin tasks not related to request I/O " +
            "(handle cluster events, refresh metadata, schedule reconnections, etc.). " +
            "If this is not set, the driver will use 2.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Long> NETTY_TIMER_TICK_DURATION = new ConfigOption<>(
        NETTY,
        "timer-tick-duration",
        "The timer tick duration in milliseconds. This is how frequent the timer should wake up to check for timed-out tasks " +
            "or speculative executions. See DataStax Java Driver option `" +
            DefaultDriverOption.NETTY_TIMER_TICK_DURATION.getPath() + "` for more information.",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<Integer> NETTY_TIMER_TICKS_PER_WHEEL = new ConfigOption<>(
        NETTY,
        "timer-ticks-per-wheel",
        "Number of ticks in a Timer wheel. See DataStax Java Driver option `" +
            DefaultDriverOption.NETTY_TIMER_TICKS_PER_WHEEL.getPath() + "` for more information.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    // Metrics

    ConfigNamespace METRICS = new ConfigNamespace(
        CQL_NS,
        "metrics",
        "Configuration options for CQL metrics");

    ConfigOption<String[]> METRICS_SESSION_ENABLED = new ConfigOption<>(
        METRICS,
        "session-enabled",
        "Comma separated list of enabled session metrics. Used only when basic metrics are enabled. " +
            "Check DataStax Cassandra Driver 4 documentation for available metrics " +
            "(example: bytes-sent, bytes-received, connected-nodes).",
        ConfigOption.Type.LOCAL,
        String[].class);

    ConfigOption<Long> METRICS_SESSION_REQUESTS_HIGHEST_LATENCY = new ConfigOption<>(
        METRICS,
        "cql-requests-highest-latency",
        "The largest latency that we expect to record for requests in milliseconds. " +
            "Used if 'cql-requests' session metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<Integer> METRICS_SESSION_REQUESTS_SIGNIFICANT_DIGITS = new ConfigOption<>(
        METRICS,
        "cql-requests-significant-digits",
        "The number of significant decimal digits to which internal structures will maintain value resolution " +
            "and separation for requests. This must be between 0 and 5. " +
            "Used if 'cql-requests' session metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Long> METRICS_SESSION_REQUESTS_REFRESH_INTERVAL = new ConfigOption<>(
        METRICS,
        "cql-requests-refresh-interval",
        "The interval at which percentile data is refreshed in milliseconds for requests. " +
            "Used if 'cql-requests' session metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<Long> METRICS_SESSION_THROTTLING_HIGHEST_LATENCY = new ConfigOption<>(
        METRICS,
        "throttling-delay-highest-latency",
        "The largest latency that we expect to record for throttling in milliseconds. " +
            "Used if 'throttling.delay' session metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<Integer> METRICS_SESSION_THROTTLING_SIGNIFICANT_DIGITS = new ConfigOption<>(
        METRICS,
        "throttling-delay-significant-digits",
        "The number of significant decimal digits to which internal structures will maintain value resolution " +
            "and separation for throttling. This must be between 0 and 5. " +
            "Used if 'throttling.delay' session metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Long> METRICS_SESSION_THROTTLING_REFRESH_INTERVAL = new ConfigOption<>(
        METRICS,
        "throttling-delay-refresh-interval",
        "The interval at which percentile data is refreshed in milliseconds for throttling. " +
            "Used if 'throttling.delay' session metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<String[]> METRICS_NODE_ENABLED = new ConfigOption<>(
        METRICS,
        "node-enabled",
        "Comma separated list of enabled node metrics. Used only when basic metrics are enabled. " +
            "Check DataStax Cassandra Driver 4 documentation for available metrics " +
            "(example: pool.open-connections, pool.available-streams, bytes-sent).",
        ConfigOption.Type.LOCAL,
        String[].class);

    ConfigOption<Long> METRICS_NODE_MESSAGES_HIGHEST_LATENCY = new ConfigOption<>(
        METRICS,
        "cql-messages-highest-latency",
        "The largest latency that we expect to record for requests in milliseconds. " +
            "Used if 'cql-messages' node metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<Integer> METRICS_NODE_MESSAGES_SIGNIFICANT_DIGITS = new ConfigOption<>(
        METRICS,
        "cql-messages-significant-digits",
        "The number of significant decimal digits to which internal structures will maintain value resolution " +
            "and separation for requests. This must be between 0 and 5. " +
            "Used if 'cql-messages' node metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Long> METRICS_NODE_MESSAGES_REFRESH_INTERVAL = new ConfigOption<>(
        METRICS,
        "cql-messages-delay-refresh-interval",
        "The interval at which percentile data is refreshed in milliseconds for requests. " +
            "Used if 'cql-messages' node metric is enabled. "+
            "See DataStax driver configuration option `"
            +DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL.getPath()+"`",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<Long> METRICS_NODE_EXPIRE_AFTER = new ConfigOption<>(
        METRICS,
        "node-expire-after",
        "The time after which the node level metrics will be evicted in milliseconds.",
        ConfigOption.Type.LOCAL,
        Long.class);

    // Request tracker (request logging)

    ConfigNamespace REQUEST_TRACKER = new ConfigNamespace(
        CQL_NS,
        "request-tracker",
        "Configuration options for CQL request tracker and builtin request logger");

    ConfigOption<String> REQUEST_TRACKER_CLASS = new ConfigOption<>(
        REQUEST_TRACKER,
        "class",
        "It is either a predefined DataStax driver value for a builtin request tracker " +
            "or a full qualified class name which implements " +
            "`com.datastax.oss.driver.internal.core.tracker.RequestTracker` interface. " +
            "If no any value provided, the default DataStax request tracker is used, which is `NoopRequestTracker` " +
            "which doesn't do anything. If `RequestLogger` value is provided, the DataStax [RequestLogger]" +
            "(https://docs.datastax.com/en/developer/java-driver/4.9/manual/core/request_tracker/#request-logger) " +
            "is used.",
        ConfigOption.Type.LOCAL,
        String.class);

    ConfigOption<Boolean> REQUEST_LOGGER_SUCCESS_ENABLED = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-success-enabled",
        "Whether to log successful requests. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigOption<Long> REQUEST_LOGGER_SLOW_THRESHOLD = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-slow-threshold",
        "The threshold to classify a successful request as `slow`. In milliseconds. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<Boolean> REQUEST_LOGGER_SLOW_ENABLED = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-slow-enabled",
        "Whether to log `slow` requests." +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigOption<Boolean> REQUEST_LOGGER_ERROR_ENABLED = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-error-enabled",
        "Whether to log failed requests." +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigOption<Integer> REQUEST_LOGGER_MAX_QUERY_LENGTH = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-max-query-length",
        "The maximum length of the query string in the log message. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Boolean> REQUEST_LOGGER_SHOW_VALUES = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-show-values",
        "Whether to log bound values in addition to the query string. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigOption<Integer> REQUEST_LOGGER_MAX_VALUE_LENGTH = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-max-value-length",
        "The maximum length for bound values in the log message. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Integer> REQUEST_LOGGER_MAX_VALUES = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-max-values",
        "The maximum number of bound values to log. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Boolean> REQUEST_LOGGER_SHOW_STACK_TRACES = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-show-stack-traces",
        "Whether to log stack traces for failed queries. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigNamespace EXECUTOR_SERVICE = new ConfigNamespace(
        CQL_NS,
        "executor-service",
        "Configuration options for CQL executor service which is used to process deserialization of CQL queries.");

    ConfigOption<Integer> EXECUTOR_SERVICE_CORE_POOL_SIZE = new ConfigOption<>(
        EXECUTOR_SERVICE,
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE.getName(),
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE.getDescription(),
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Integer> EXECUTOR_SERVICE_MAX_POOL_SIZE = new ConfigOption<>(
        EXECUTOR_SERVICE,
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_MAX_POOL_SIZE.getName(),
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_MAX_POOL_SIZE.getDescription(),
        ConfigOption.Type.LOCAL,
        Integer.class,
        Integer.MAX_VALUE);

    ConfigOption<Long> EXECUTOR_SERVICE_KEEP_ALIVE_TIME = new ConfigOption<>(
        EXECUTOR_SERVICE,
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_KEEP_ALIVE_TIME.getName(),
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_KEEP_ALIVE_TIME.getDescription(),
        ConfigOption.Type.LOCAL,
        Long.class,
        60000L);

    ConfigOption<String> EXECUTOR_SERVICE_CLASS = new ConfigOption<>(
        EXECUTOR_SERVICE,
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.getName(),
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.getDescription(),
        ConfigOption.Type.LOCAL,
        String.class,
        ExecutorServiceBuilder.FIXED_THREAD_POOL_CLASS);

    ConfigOption<Long> EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME = new ConfigOption<>(
        EXECUTOR_SERVICE,
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME.getName(),
        GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME.getDescription(),
        ConfigOption.Type.LOCAL,
        Long.class,
        60000L);

    ConfigOption<Integer> SESSION_LEAK_THRESHOLD = new ConfigOption<>(
        CQL_NS,
        "session-leak-threshold",
        "The maximum number of live sessions that are allowed to coexist in a given VM until the warning " +
            "starts to log for every new session. " +
            "If the value is less than or equal to 0, the feature is disabled: no warning will be issued. "+
            "See DataStax Java Driver option `" +
            DefaultDriverOption.SESSION_LEAK_THRESHOLD.getPath() + "` for more information.",
        ConfigOption.Type.MASKABLE, Integer.class);

    ConfigOption<Long> REQUEST_TIMEOUT = new ConfigOption<>(
        CQL_NS,
        "request-timeout",
        "Timeout for CQL requests in milliseconds. See DataStax Java Driver option `" +
            DefaultDriverOption.REQUEST_TIMEOUT.getPath() + "` for more information.",
        ConfigOption.Type.MASKABLE, 12000L);

    ConfigNamespace INTERNAL_CONFIGURATION = new ConfigNamespace(
        CQL_NS,
        "internal",
        "Advanced configuration of internal DataStax driver. Notice, all available configurations will be composed in the order. " +
            "Non specified configurations will be skipped. By default only base configuration is enabled " +
            "(which has the smallest priority. It means that you can overwrite any configuration used in base programmatic configuration by using any other configuration type). " +
            "The configurations are composed in the next order (sorted by priority in descending order): " +
            "`file-configuration`, " +
            "`resource-configuration`, " +
            "`string-configuration`, " +
            "`url-configuration`, " +
            "`base-programmatic-configuration` (which is controlled by `base-programmatic-configuration-enabled` property). " +
            "Configurations with higher priority always overwrite configurations with lower priority. " +
            "I.e. if the same configuration parameter is used in both `file-configuration` and `string-configuration` the configuration parameter " +
            "from `file-configuration` will be used and configuration parameter from `string-configuration` will be ignored. "+
            "See available configuration options and configurations structure here: https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/configuration/reference/");

    ConfigOption<Boolean> BASE_PROGRAMMATIC_CONFIGURATION_ENABLED = new ConfigOption<>(
        INTERNAL_CONFIGURATION,
        "base-programmatic-configuration-enabled",
        "Whether to use main programmatic configuration provided by JanusGraph properties or not. " +
            "We don't recommend to disable this property unless you want to disable usage of all "+
            CQL_NS.toStringWithoutRoot()+" properties and use default configurations or other configurations. " +
            "If programmatic configuration options miss some important configuration options you can provide those missing " +
            "configurations with other configuration types which will be applied with programmatic configuration " +
            "(see other configuration types in this section). For most use cases this option should always be `true`. " +
            "JanusGraph behaviour might be unpredictable when using unspecified configuration options.",
        ConfigOption.Type.MASKABLE,
        true);

    ConfigOption<String> URL_CONFIGURATION = new ConfigOption<>(
        INTERNAL_CONFIGURATION,
        "url-configuration",
        "Url where to get DataStax configuration.",
        ConfigOption.Type.MASKABLE,
        String.class);

    ConfigOption<String> STRING_CONFIGURATION = new ConfigOption<>(
        INTERNAL_CONFIGURATION,
        "string-configuration",
        "String representing DataStax configuration.",
        ConfigOption.Type.MASKABLE,
        String.class);

    ConfigOption<String> RESOURCE_CONFIGURATION = new ConfigOption<>(
        INTERNAL_CONFIGURATION,
        "resource-configuration",
        "Classpath resource with DataStax configuration.",
        ConfigOption.Type.MASKABLE,
        String.class);

    ConfigOption<String> FILE_CONFIGURATION = new ConfigOption<>(
        INTERNAL_CONFIGURATION,
        "file-configuration",
        "Path to file with DataStax configuration.",
        ConfigOption.Type.LOCAL,
        String.class);

    ConfigNamespace CQL_GROUPING_NS = new ConfigNamespace(
        CQL_NS,
        "grouping",
        "Configuration options for controlling CQL queries grouping");

    String MAX_IN_CONFIG_MSG = "Notice, for ScyllaDB this option should not exceed the maximum number of distinct " +
        "clustering key restrictions per query which can be changed by ScyllaDB configuration option `max-partition-key-restrictions-per-query` " +
        "(https://enterprise.docs.scylladb.com/branch-2022.2/faq.html#how-can-i-change-the-maximum-number-of-in-restrictions). " +
        "For AstraDB this limit is set to 20 and usually it's fixed. However, you can ask customer support for a possibility to change " +
        "the default threshold to your desired configuration via `partition_keys_in_select_failure_threshold` " +
        "and `in_select_cartesian_product_failure_threshold` threshold configurations (https://docs.datastax.com/en/astra-serverless/docs/plan/planning.html#_cassandra_yaml).<br>" +
        "Ensure that your storage backend allows more IN selectors than the one set via this configuration.";

    ConfigOption<Boolean> SLICE_GROUPING_ALLOWED = new ConfigOption<>(
        CQL_GROUPING_NS,
        "slice-allowed",
        "If `true` this allows multiple Slice queries which are allowed to be performed as non-range " +
            "queries (i.e. direct equality operation) to be grouped together into a single CQL query via `IN` operator. " +
            "Notice, currently only operations to fetch properties with Cardinality.SINGLE are allowed to be performed as non-range queries " +
            "(edges fetching or properties with Cardinality SET or LIST won't be grouped together).<br>" +
            "If this option is `false` then each Slice query will be executed in a separate asynchronous CQL query even when " +
            "grouping is allowed.",
        ConfigOption.Type.MASKABLE,
        true);

    ConfigOption<Integer> SLICE_GROUPING_LIMIT = new ConfigOption<>(
        CQL_GROUPING_NS,
        "slice-limit",
        "Maximum amount of grouped together slice queries into a single CQL query.<br>" + MAX_IN_CONFIG_MSG +
            "<br>This option is used only when `"+SLICE_GROUPING_ALLOWED.toStringWithoutRoot()+"` is `true`.",
        ConfigOption.Type.MASKABLE,
        20);

    ConfigOption<Boolean> KEYS_GROUPING_ALLOWED = new ConfigOption<>(
        CQL_GROUPING_NS,
        "keys-allowed",
        "If `true` this allows multiple partition keys to be grouped together into a single CQL query via `IN` operator based on the keys grouping strategy provided " +
            "(usually grouping is done by same token-ranges or same replica sets, but may also involve shard ids for custom implementations).<br>" +
            "Notice, that any CQL query grouped with more than 1 key will require to return a row key for any column fetched.<br>" +
            "This option is useful when less amount of CQL queries is desired to be sent for read requests in expense of fetching more data (partition key per each fetched value).<br>" +
            "Notice, different storage backends may have different way of executing multi-partition `IN` queries " +
            "(including, but not limited to how the checksum queries are sent for different consistency levels, processing node CPU usage, disk access pattern, etc.). Thus, a proper " +
            "benchmarking is needed to determine if keys grouping is useful or not per case by case scenario.<br>" +
            "This option can be enabled only for storage backends which support `PER PARTITION LIMIT`. As such, this feature can't be used with Amazon Keyspaces because it doesn't support `PER PARTITION LIMIT`.<br>" +
            "If this option is `false` then each partition key will be executed in a separate asynchronous CQL query even when multiple keys from the same token range are queried.<br>" +
            "Notice, the default grouping strategy does not take shards into account. Thus, this might be inefficient with ScyllaDB storage backend. " +
            "ScyllaDB specific keys grouping strategy should be implemented after the resolution of the [ticket #232](https://github.com/scylladb/java-driver/issues/232).",
        ConfigOption.Type.MASKABLE,
        false);

    ConfigOption<String> KEYS_GROUPING_CLASS = new ConfigOption<>(
        CQL_GROUPING_NS,
        "keys-class",
        "Full class path of the keys grouping execution strategy. The class should implement " +
            "`org.janusgraph.diskstorage.cql.strategy.GroupedExecutionStrategy` interface and have a public constructor " +
            "with two arguments `org.janusgraph.diskstorage.configuration.Configuration` and `org.janusgraph.diskstorage.cql.CQLStoreManager`.<br>" +
            "Shortcuts available:<br>" +
            "- `"+GroupedExecutionStrategyBuilder.TOKEN_RANGE_AWARE +"` - groups partition keys which belong to the same token range. Notice, this strategy does not take shards into account. Thus, this might be inefficient with ScyllaDB storage backend.<br>" +
            "- `"+GroupedExecutionStrategyBuilder.REPLICAS_AWARE +"` - groups partition keys which belong to the same replica sets (same nodes). Notice, this strategy does not take shards into account. Thus, this might be inefficient with ScyllaDB storage backend.<br>" +
            "<br>Usually `"+GroupedExecutionStrategyBuilder.TOKEN_RANGE_AWARE+"` grouping strategy provides more smaller groups where each group contain keys which are stored close to each other on a disk and may cause less disk seeks in some cases. " +
            "However `"+GroupedExecutionStrategyBuilder.REPLICAS_AWARE+"` grouping strategy groups keys per replica set which usually means fewer bigger groups to be used (i.e. less CQL requests)." +
            "<br>This option takes effect only when `"+KEYS_GROUPING_ALLOWED.toStringWithoutRoot()+"` is `true`.",
        ConfigOption.Type.MASKABLE,
        GroupedExecutionStrategyBuilder.REPLICAS_AWARE);

    ConfigOption<Integer> KEYS_GROUPING_LIMIT = new ConfigOption<>(
        CQL_GROUPING_NS,
        "keys-limit",
        "Maximum amount of the keys which can be grouped together into a single CQL query. " +
            "If more keys are queried, they are going to be grouped into separate CQL queries.<br>"
            + MAX_IN_CONFIG_MSG +
            "<br>This option takes effect only when `"+KEYS_GROUPING_ALLOWED.toStringWithoutRoot()+"` is `true`.",
        ConfigOption.Type.MASKABLE,
        20);

    ConfigOption<Integer> KEYS_GROUPING_MIN = new ConfigOption<>(
        CQL_GROUPING_NS,
        "keys-min",
        "Minimum amount of keys to consider for grouping. Grouping will be skipped for any multi-key query " +
            "which has less than this amount of keys (i.e. a separate CQL query will be executed for each key in such case).<br>" +
            "Usually this configuration should always be set to `2`. It is useful to increase the value only in cases when queries " +
            "with more keys should not be grouped, but be performed separately to increase parallelism in expense of the network overhead." +
            "<br>This option takes effect only when `"+KEYS_GROUPING_ALLOWED.toStringWithoutRoot()+"` is `true`.",
        ConfigOption.Type.MASKABLE,
        2);
}
