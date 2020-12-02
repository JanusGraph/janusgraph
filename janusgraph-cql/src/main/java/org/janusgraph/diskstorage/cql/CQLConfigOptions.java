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
    ConfigOption<Integer> REPLICATION_FACTOR = new ConfigOption<>(
            CQL_NS,
            "replication-factor",
            "The number of data replicas (including the original copy) that should be kept",
            ConfigOption.Type.GLOBAL_OFFLINE,
            1);

    ConfigOption<String> REPLICATION_STRATEGY = new ConfigOption<>(
            CQL_NS,
            "replication-strategy-class",
            "The replication strategy to use for JanusGraph keyspace",
            ConfigOption.Type.FIXED,
            "SimpleStrategy");

    ConfigOption<String[]> REPLICATION_OPTIONS = new ConfigOption<>(
            CQL_NS,
            "replication-strategy-options",
            "Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a " +
                    "map.  It must have an even number of elements in [key,val,key,val,...] form.  A replication_factor set " +
                    "here takes precedence over one set with " + ConfigElement.getPath(REPLICATION_FACTOR),
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
            ConfigOption.Type.FIXED,
            1);

    ConfigOption<Integer> REMOTE_MAX_CONNECTIONS_PER_HOST = new ConfigOption<>(
            CQL_NS,
            "remote-max-connections-per-host",
            "The maximum number of connections that can be created per host for remote datacenter",
            ConfigOption.Type.FIXED,
            1);

    ConfigOption<Integer> MAX_REQUESTS_PER_CONNECTION = new ConfigOption<>(
            CQL_NS,
            "max-requests-per-connection",
            "The maximum number of requests that can be executed concurrently on a connection.",
            ConfigOption.Type.FIXED,
            1024);

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
            "If this is set to 0, the driver will use `Runtime.getRuntime().availableProcessors() * 2`.",
        ConfigOption.Type.LOCAL,
        Integer.class,
        0);

    ConfigOption<Integer> NETTY_ADMIN_SIZE = new ConfigOption<>(
        NETTY,
        "admin-size",
        "The number of threads for the event loop group used for admin tasks not related to request I/O " +
            "(handle cluster events, refresh metadata, schedule reconnections, etc.). " +
            "If this is set to 0, the driver will use `Runtime.getRuntime().availableProcessors() * 2`.",
        ConfigOption.Type.LOCAL,
        Integer.class,
        0);

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
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigOption<Long> REQUEST_LOGGER_SLOW_THRESHOLD = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-slow-threshold",
        "The threshold to classify a successful request as `slow`. In milliseconds. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Long.class);

    ConfigOption<Boolean> REQUEST_LOGGER_SLOW_ENABLED = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-slow-enabled",
        "Whether to log `slow` requests." +
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigOption<Boolean> REQUEST_LOGGER_ERROR_ENABLED = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-error-enabled",
        "Whether to log failed requests." +
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigOption<Integer> REQUEST_LOGGER_MAX_QUERY_LENGTH = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-max-query-length",
        "The maximum length of the query string in the log message. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Boolean> REQUEST_LOGGER_SHOW_VALUES = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-show-values",
        "Whether to log bound values in addition to the query string. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);

    ConfigOption<Integer> REQUEST_LOGGER_MAX_VALUE_LENGTH = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-max-value-length",
        "The maximum length for bound values in the log message. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Integer> REQUEST_LOGGER_MAX_VALUES = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-max-values",
        "The maximum number of bound values to log. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Integer.class);

    ConfigOption<Boolean> REQUEST_LOGGER_SHOW_STACK_TRACES = new ConfigOption<>(
        REQUEST_TRACKER,
        "logs-show-stack-traces",
        "Whether to log stack traces for failed queries. " +
            "Can be used when `" + REQUEST_TRACKER_CLASS.toString() + "` is set to `RequestLogger`.",
        ConfigOption.Type.LOCAL,
        Boolean.class);
}
