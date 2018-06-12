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

package org.janusgraph.graphdb.configuration;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.ttl.TTLKCVSManager;
import org.janusgraph.graphdb.tinkerpop.JanusGraphDefaultSchemaMaker;
import org.janusgraph.graphdb.tinkerpop.Tp3DefaultSchemaMaker;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.types.typemaker.DisableDefaultSchemaMaker;
import org.janusgraph.util.stats.NumberUtil;
import org.janusgraph.diskstorage.util.time.*;
import org.janusgraph.diskstorage.configuration.*;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode;
import org.janusgraph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.graphdb.database.cache.MetricInstrumentedSchemaCache;
import org.janusgraph.graphdb.database.cache.StandardSchemaCache;
import org.janusgraph.graphdb.database.cache.SchemaCache;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.util.encoding.LongEncoding;
import org.janusgraph.util.system.ConfigurationUtil;
import org.janusgraph.util.system.LoggerUtil;
import org.janusgraph.util.system.NetworkUtil;

import org.apache.tinkerpop.gremlin.structure.Graph;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.management.MBeanServerFactory;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.graphdb.database.idassigner.VertexIDAssigner;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.janusgraph.util.stats.MetricManager;

/**
 * Provides functionality to configure a {@link org.janusgraph.core.JanusGraph} INSTANCE.
 * <p>
 * <p>
 * A graph database configuration is uniquely associated with a graph database and must not be used for multiple
 * databases.
 * <p>
 * After a graph database has been initialized with respect to a configuration, some parameters of graph database
 * configuration may no longer be modifiable.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class GraphDatabaseConfiguration {

    private static final Logger log =
            LoggerFactory.getLogger(GraphDatabaseConfiguration.class);


    public static final ConfigNamespace ROOT_NS = new ConfigNamespace(null,"root","Root Configuration Namespace for the JanusGraph Graph Database");

    // ########## Graph-level Config Options ##########
    // ################################################

    public static final ConfigNamespace GRAPH_NS = new ConfigNamespace(ROOT_NS,"graph",
            "General configuration options");

    public static final ConfigOption<Boolean> ALLOW_SETTING_VERTEX_ID = new ConfigOption<>(GRAPH_NS,"set-vertex-id",
            "Whether user provided vertex ids should be enabled and JanusGraph's automatic id allocation be disabled. " +
            "Useful when operating JanusGraph in concert with another storage system that assigns long ids but disables some " +
            "of JanusGraph's advanced features which can lead to inconsistent data. EXPERT FEATURE - USE WITH GREAT CARE.",
            ConfigOption.Type.FIXED, false);

    public static final ConfigOption<String> GRAPH_NAME = new ConfigOption<>(GRAPH_NS, "graphname",
            "This config option is an optional configuration setting that you may supply when opening a graph. " +
            "The String value you provide will be the name of your graph. If you use the ConfigurationManagement APIs, " +
            "then you will be able to access your graph by this String representation using the ConfiguredGraphFactory APIs.",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<TimestampProviders> TIMESTAMP_PROVIDER = new ConfigOption<>(GRAPH_NS, "timestamps",
            "The timestamp resolution to use when writing to storage and indices. Sets the time granularity for the " +
            "entire graph cluster. To avoid potential inaccuracies, the configured time resolution should match " +
            "those of the backend systems. Some JanusGraph storage backends declare a preferred timestamp resolution that " +
            "reflects design constraints in the underlying service. When the backend provides " +
            "a preferred default, and when this setting is not explicitly declared in the config file, the backend " +
            "default is used and the general default associated with this setting is ignored.  An explicit " +
            "declaration of this setting overrides both the general and backend-specific defaults.",
            ConfigOption.Type.FIXED, TimestampProviders.class, TimestampProviders.MICRO);

//    public static final ConfigOption<KryoInstanceCacheImpl> KRYO_INSTANCE_CACHE = new ConfigOption<>(GRAPH_NS, "kryo-instance-cache",
//            "Controls how Kryo instances are created and cached.  Kryo instances are not " +
//            "safe for concurrent access.  JanusGraph is responsible guaranteeing that concurrent threads use separate " +
//            "Kryo instances.  JanusGraph defaults to a Kryo caching approach based on ThreadLocal, as recommended by the " +
//            "Kryo documentation (https://github.com/EsotericSoftware/kryo#threading).  " +
//            "However, these ThreadLocals are not necessarily removed when JanusGraph shuts down.  When JanusGraph runs on an " +
//            "externally-controlled thread pool that reuses threads indefinitely, such as that provided by Tomcat, " +
//            "these unremoved ThreadLocals can potentially cause unintended reference retention for as long as the " +
//            "affected threads remain alive.  In that type of execution environment, consider setting this to " +
//            "CONCURRENT_HASH_MAP.  The CHM implementation releases all references when JanusGraph is shutdown, but it " +
//            "also subject to some synchronization-related performance overhead that the ThreadLocal-based default " +
//            "implementation avoids.  Recent versions of Kryo include a class called KryoPool that offers another way " +
//            "to solve this problem.  However, KryoPool is not supported in older deployments because the version of Kryo " +
//            "used by older deployments predates KryoPool's introduction.",
//            ConfigOption.Type.MASKABLE, KryoInstanceCacheImpl.class, KryoInstanceCacheImpl.THREAD_LOCAL);

    public static final ConfigOption<String> UNIQUE_INSTANCE_ID = new ConfigOption<>(GRAPH_NS,"unique-instance-id",
            "Unique identifier for this JanusGraph instance.  This must be unique among all instances " +
            "concurrently accessing the same stores or indexes.  It's automatically generated by " +
            "concatenating the hostname, process id, and a static (process-wide) counter. " +
            "Leaving it unset is recommended.",
            ConfigOption.Type.LOCAL, String.class);


    public static final ConfigOption<Short> UNIQUE_INSTANCE_ID_SUFFIX = new ConfigOption<>(GRAPH_NS,"unique-instance-id-suffix",
            "When this is set and " + UNIQUE_INSTANCE_ID.getName() + " is not, this JanusGraph " +
            "instance's unique identifier is generated by concatenating the hex encoded hostname to the " +
            "provided number.",
            ConfigOption.Type.LOCAL, Short.class);

    public static final ConfigOption<String> INITIAL_JANUSGRAPH_VERSION = new ConfigOption<>(GRAPH_NS,"janusgraph-version",
            "The version of JanusGraph with which this database was created. Automatically set on first start. Don't manually set this property.",
            ConfigOption.Type.FIXED, String.class).hide();

    public static final ConfigOption<Boolean> UNIQUE_INSTANCE_ID_HOSTNAME = new ConfigOption<Boolean>(GRAPH_NS,"use-hostname-for-unique-instance-id",
            "When this is set, this JanusGraph's unique instance identifier is set to the hostname. If " + UNIQUE_INSTANCE_ID_SUFFIX.getName() +
            " is also set, then the identifier is set to <hostname><suffix>.",
            ConfigOption.Type.LOCAL, Boolean.class, false);

    public static final ConfigOption<Boolean> REPLACE_INSTANCE_IF_EXISTS = new ConfigOption<Boolean>(GRAPH_NS,"replace-instance-if-exists",
            "If a JanusGraph instance with the same instance identifier already exists, the usage of this " +
            "configuration option results in the opening of this graph anwyay.",
            ConfigOption.Type.LOCAL, Boolean.class, false);

    public static final ConfigOption<String> TITAN_COMPATIBLE_VERSIONS = new ConfigOption<>(GRAPH_NS,"titan-version",
            "Titan version for backwards compatibility which this database was created. Automatically set on first start. Don't manually set this property.",
            ConfigOption.Type.FIXED, String.class).hide();

    public static final ConfigOption<Boolean> ALLOW_STALE_CONFIG = new ConfigOption<>(GRAPH_NS,"allow-stale-config",
            "Whether to allow the local and storage-backend-hosted copies of the configuration to contain conflicting values for " +
            "options with any of the following types: " + Joiner.on(", ").join(ConfigOption.getManagedTypes()) + ".  " +
            "These types are managed globally through the storage backend and cannot be overridden by changing the " +
            "local configuration.  This type of conflict usually indicates misconfiguration.  When this option is true, " +
            "JanusGraph will log these option conflicts, but continue normal operation using the storage-backend-hosted value " +
            "for each conflicted option.  When this option is false, JanusGraph will log these option conflicts, but then it " +
            "will throw an exception, refusing to start.",
            ConfigOption.Type.MASKABLE, Boolean.class, true);

    // ################ INSTANCE REGISTRATION (system) #######################
    // ##############################################################

    public static final ConfigNamespace REGISTRATION_NS = new ConfigNamespace(ROOT_NS,"system-registration",
            "This is used internally to keep track of open instances.",true);

    public static final ConfigOption<Instant> REGISTRATION_TIME = new ConfigOption<>(REGISTRATION_NS,"startup-time",
            "Timestamp when this instance was started.  Automatically set.", ConfigOption.Type.GLOBAL, Instant.class).hide();


    // ########## OLAP Style Processing ##########
    // ################################################

    public static final ConfigNamespace JOB_NS = new ConfigNamespace(null,"job","Root Configuration Namespace for JanusGraph OLAP jobs");

    public static final ConfigOption<Long> JOB_START_TIME = new ConfigOption<>(JOB_NS,"start-time",
            "Timestamp (ms since epoch) when the job started. Automatically set.", ConfigOption.Type.LOCAL, Long.class).hide();


    public static final ConfigNamespace COMPUTER_NS = new ConfigNamespace(ROOT_NS,"computer",
            "GraphComputer related configuration");

    public static final ConfigOption<String> COMPUTER_RESULT_MODE = new ConfigOption<>(COMPUTER_NS,"result-mode",
            "How the graph computer should return the computed results. 'persist' for writing them into the graph, " +
                    "'localtx' for writing them into the local transaction, or 'none' (default)", ConfigOption.Type.MASKABLE, "none");


    // ################ Transaction #######################
    // ################################################

    public static final ConfigNamespace TRANSACTION_NS = new ConfigNamespace(ROOT_NS,"tx",
            "Configuration options for transaction handling");

    public static final ConfigOption<Boolean> SYSTEM_LOG_TRANSACTIONS = new ConfigOption<>(TRANSACTION_NS,"log-tx",
            "Whether transaction mutations should be logged to JanusGraph's write-ahead transaction log which can be used for recovery of partially failed transactions",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Duration> MAX_COMMIT_TIME = new ConfigOption<>(TRANSACTION_NS,"max-commit-time",
            "Maximum time (in ms) that a transaction might take to commit against all backends. This is used by the distributed " +
                    "write-ahead log processing to determine when a transaction can be considered failed (i.e. after this time has elapsed)." +
                    "Must be longer than the maximum allowed write time.",
            ConfigOption.Type.GLOBAL, Duration.ofSeconds(10));


    public static final ConfigNamespace TRANSACTION_RECOVERY_NS = new ConfigNamespace(TRANSACTION_NS,"recovery",
            "Configuration options for transaction recovery processes");

    public static final ConfigOption<Boolean> VERBOSE_TX_RECOVERY = new ConfigOption<>(TRANSACTION_RECOVERY_NS,"verbose",
            "Whether the transaction recovery system should print recovered transactions and other activity to standard output",
            ConfigOption.Type.MASKABLE, false);

    // ################ Query Processing #######################
    // ################################################

    public static final ConfigNamespace QUERY_NS = new ConfigNamespace(ROOT_NS,"query",
            "Configuration options for query processing");

    public static final ConfigOption<Boolean> IGNORE_UNKNOWN_INDEX_FIELD = new ConfigOption<>(QUERY_NS, "ignore-unknown-index-key",
            "Whether to ignore undefined types encountered in user-provided index queries",
            ConfigOption.Type.MASKABLE, false);

    public static final String UNKNOWN_FIELD_NAME = "unknown_key";


    public static final ConfigOption<Boolean> FORCE_INDEX_USAGE = new ConfigOption<>(QUERY_NS,"force-index",
            "Whether JanusGraph should throw an exception if a graph query cannot be answered using an index. Doing so" +
                    "limits the functionality of JanusGraph's graph queries but ensures that slow graph queries are avoided " +
                    "on large graphs. Recommended for production use of JanusGraph.",
            ConfigOption.Type.MASKABLE, false);


    public static final ConfigOption<Boolean> PROPERTY_PREFETCHING = new ConfigOption<>(QUERY_NS,"fast-property",
            "Whether to pre-fetch all properties on first singular vertex property access. This can eliminate backend calls on subsequent" +
                    "property access for the same vertex at the expense of retrieving all properties at once. This can be " +
                    "expensive for vertices with many properties",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> ADJUST_LIMIT = new ConfigOption<>(QUERY_NS,"smart-limit",
            "Whether the query optimizer should try to guess a smart limit for the query to ensure responsiveness in " +
                    "light of possibly large result sets. Those will be loaded incrementally if this option is enabled.",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> USE_MULTIQUERY = new ConfigOption<>(QUERY_NS,"batch",
            "Whether traversal queries should be batched when executed against the storage backend. This can lead to significant " +
                    "performance improvement if there is a non-trivial latency to the backend.",
            ConfigOption.Type.MASKABLE, false);

    // ################ SCHEMA #######################
    // ################################################

    public static final ConfigNamespace SCHEMA_NS = new ConfigNamespace(ROOT_NS,"schema",
            "Schema related configuration options");

    public static final ConfigOption<String> AUTO_TYPE = new ConfigOption<>(SCHEMA_NS,"default",
            "Configures the DefaultSchemaMaker to be used by this graph. If set to 'none', automatic schema creation is disabled. " +
                    "Defaults to a blueprints compatible schema maker with MULTI edge labels and SINGLE property keys",
            ConfigOption.Type.MASKABLE, "default", new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
            if (s==null) return false;
            if (preregisteredAutoType.containsKey(s)) return true;
            try {
                Class<?> clazz = ClassUtils.getClass(s);
                return DefaultSchemaMaker.class.isAssignableFrom(clazz);
            } catch (ClassNotFoundException e) {
                return false;
            }
        }
    });

    private static final Map<String, DefaultSchemaMaker> preregisteredAutoType =
            ImmutableMap.of("none", DisableDefaultSchemaMaker.INSTANCE,
                    "default", JanusGraphDefaultSchemaMaker.INSTANCE,
                    "tp3", Tp3DefaultSchemaMaker.INSTANCE);

    public static final ConfigOption<Boolean> SCHEMA_CONSTRAINTS = new ConfigOption<>(SCHEMA_NS, "constraints",
            "Configures the schema constraints to be used by this graph. If config 'schema.constraints' " +
            "is set to 'true' and 'schema.default' is set to 'none', then an 'IllegalArgumentException' is thrown for schema constraint violations. " +
            "If 'schema.constraints' is set to 'true' and 'schema.default' is not set 'none', schema constraints are automatically created "+
            "as described in the config option 'schema.default'. If 'schema.constraints' is set to 'false' which is the default, then no schema constraints are applied.",
            ConfigOption.Type.GLOBAL_OFFLINE, false);

    // ################ CACHE #######################
    // ################################################

    public static final ConfigNamespace CACHE_NS = new ConfigNamespace(ROOT_NS,"cache","Configuration options that modify JanusGraph's caching behavior");


    public static final ConfigOption<Boolean> DB_CACHE = new ConfigOption<>(CACHE_NS,"db-cache",
            "Whether to enable JanusGraph's database-level cache, which is shared across all transactions. " +
            "Enabling this option speeds up traversals by holding hot graph elements in memory, " +
            "but also increases the likelihood of reading stale data.  Disabling it forces each " +
            "transaction to independently fetch graph elements from storage before reading/writing them.",
            ConfigOption.Type.MASKABLE, false);

    /**
     * The size of the database level cache.
     * If this value is between 0.0 (strictly bigger) and 1.0 (strictly smaller), then it is interpreted as a
     * percentage of the total heap space available to the JVM this JanusGraph instance is running in.
     * If this value is bigger than 1.0 it is interpreted as an absolute size in bytes.
     */
    public static final ConfigOption<Double> DB_CACHE_SIZE = new ConfigOption<>(CACHE_NS,"db-cache-size",
            "Size of JanusGraph's database level cache.  Values between 0 and 1 are interpreted as a percentage " +
            "of VM heap, while larger values are interpreted as an absolute size in bytes.",
            ConfigOption.Type.MASKABLE, 0.3);

    /**
     * How long the database level cache will keep keys expired while the mutations that triggered the expiration
     * are being persisted. This value should be larger than the time it takes for persisted mutations to become visible.
     * This setting only ever makes sense for distributed storage backends where writes may be accepted but are not
     * immediately readable.
     */
    public static final ConfigOption<Integer> DB_CACHE_CLEAN_WAIT = new ConfigOption<>(CACHE_NS,"db-cache-clean-wait",
            "How long, in milliseconds, database-level cache will keep entries after flushing them.  " +
            "This option is only useful on distributed storage backends that are capable of acknowledging writes " +
            "without necessarily making them immediately visible.",
            ConfigOption.Type.GLOBAL_OFFLINE, 50);

    /**
     * The default expiration time for elements held in the database level cache. This is the time period before
     * JanusGraph will check against storage backend for a newer query answer.
     * Setting this value to 0 will cache elements forever (unless they get evicted due to space constraints). This only
     * makes sense when this is the only JanusGraph instance interacting with a storage backend.
     */
    public static final ConfigOption<Long> DB_CACHE_TIME = new ConfigOption<>(CACHE_NS,"db-cache-time",
            "Default expiration time, in milliseconds, for entries in the database-level cache. " +
            "Entries are evicted when they reach this age even if the cache has room to spare. " +
            "Set to 0 to disable expiration (cache entries live forever or until memory pressure " +
            "triggers eviction when set to 0).",
            ConfigOption.Type.GLOBAL_OFFLINE, 10000L);

    /**
     * Configures the maximum number of recently-used vertices cached by a transaction. The smaller the cache size, the
     * less memory a transaction can consume at maximum. For many concurrent, long running transactions in memory constraint
     * environments, reducing the cache size can avoid OutOfMemory and GC limit exceeded exceptions.
     * Note, however, that all modifications in a transaction must always be kept in memory and hence this setting does not
     * have much impact on write intense transactions. Those must be split into smaller transactions in the case of memory errors.
     * <p>
     * The recently-used vertex cache can contain both dirty and clean vertices, that is, both vertices which have been
     * created or updated in the current transaction and vertices which have only been read in the current transaction.
     */
    public static final ConfigOption<Integer> TX_CACHE_SIZE = new ConfigOption<>(CACHE_NS,"tx-cache-size",
            "Maximum size of the transaction-level cache of recently-used vertices.",
            ConfigOption.Type.MASKABLE, 20000);

    /**
     * Configures the initial size of the dirty (modified) vertex map used by a transaction.  All vertices created or
     * updated by a transaction are held in that transaction's dirty vertex map until the transaction commits.
     * This option sets the initial size of the dirty map.  Unlike {@link #TX_CACHE_SIZE}, this is not a maximum.
     * The transaction will transparently allocate more space to store dirty vertices if this initial size hint
     * is exceeded.  Transactions that know how many vertices they are likely to modify a priori can avoid resize
     * costs associated with growing the dirty vertex data structure by setting this option.
     */
    public static final ConfigOption<Integer> TX_DIRTY_SIZE = new ConfigOption<>(CACHE_NS, "tx-dirty-size",
          "Initial size of the transaction-level cache of uncommitted dirty vertices. " +
          "This is a performance hint for write-heavy, performance-sensitive transactional workloads. " +
          "If set, it should roughly match the median vertices modified per transaction.",
          ConfigOption.Type.MASKABLE, Integer.class);

    /**
     * The default value of {@link #TX_DIRTY_SIZE} when batch loading is disabled.
     * This value is only considered if the user does not specify a value for
     * {@code #TX_DIRTY_CACHE_SIZE} explicitly in either the graph or transaction config.
     */
    private static final int TX_DIRTY_SIZE_DEFAULT_WITHOUT_BATCH = 32;

    /**
     * The default value of {@link #TX_DIRTY_SIZE} when batch loading is enabled.
     * This value is only considered if the user does not specify a value for
     * {@code #TX_DIRTY_CACHE_SIZE} explicitly in either the graph or transaction config.
     */
    private static final int TX_DIRTY_SIZE_DEFAULT_WITH_BATCH = 4096;


    // ################ STORAGE #######################
    // ################################################

    public static final ConfigNamespace STORAGE_NS = new ConfigNamespace(ROOT_NS,"storage","Configuration options for the storage backend.  Some options are applicable only for certain backends.");

    /**
     * Storage root directory for those storage backends that require local storage
     */
    public static final ConfigOption<String> STORAGE_ROOT = new ConfigOption<>(STORAGE_NS,"root",
            "Storage root directory for those storage backends that require local storage. " +
            "If you do not supply storage.directory and you do supply graph.graphname, then your data " +
            "will be stored in the directory equivalent to <STORAGE_ROOT>/<GRAPH_NAME>.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Storage directory for those storage backends that require local storage
     */
    public static final ConfigOption<String> STORAGE_DIRECTORY = new ConfigOption<>(STORAGE_NS,"directory",
            "Storage directory for those storage backends that require local storage.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Path to a configuration file for those storage backends that
     * require/support a separate config file
     */
    public static final ConfigOption<String> STORAGE_CONF_FILE = new ConfigOption<>(STORAGE_NS,"conf-file",
            "Path to a configuration file for those storage backends which require/support a single separate config file.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Define the storage backed to use for persistence
     */
    public static final ConfigOption<String> STORAGE_BACKEND = new ConfigOption<>(STORAGE_NS,"backend",
            "The primary persistence provider used by JanusGraph.  This is required.  It should be set one of " +
            "JanusGraph's built-in shorthand names for its standard storage backends " +
            "(shorthands: " + Joiner.on(", ").join(StandardStoreManager.getAllShorthands()) + ") " +
            "or to the full package and classname of a custom/third-party StoreManager implementation.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Specifies whether this database is read-only, i.e. write operations are not supported
     */
    public static final ConfigOption<Boolean> STORAGE_READONLY = new ConfigOption<>(STORAGE_NS,"read-only",
            "Read-only database",
            ConfigOption.Type.LOCAL, false);

    /**
     * Enables batch loading which improves write performance but assumes that only one thread is interacting with
     * the graph
     */
    public static final ConfigOption<Boolean> STORAGE_BATCH = new ConfigOption<>(STORAGE_NS,"batch-loading",
            "Whether to enable batch loading into the storage backend",
            ConfigOption.Type.LOCAL, false);

    /**
     * Enables transactions on storage backends that support them
     */
    public static final ConfigOption<Boolean> STORAGE_TRANSACTIONAL = new ConfigOption<>(STORAGE_NS,"transactions",
            "Enables transactions on storage backends that support them",
            ConfigOption.Type.MASKABLE, true);

    /**
     * Buffers graph mutations locally up to the specified number before persisting them against the storage backend.
     * Set to 0 to disable buffering. Buffering is disabled automatically if the storage backend does not support buffered mutations.
     */
    public static final ConfigOption<Integer> BUFFER_SIZE = new ConfigOption<>(STORAGE_NS,"buffer-size",
            "Size of the batch in which mutations are persisted",
            ConfigOption.Type.MASKABLE, 1024, ConfigOption.positiveInt());

    /*
     * Number of times the database attempts to persist the transactional state to the storage layer.
     * Persisting the state of a committed transaction might fail for various reasons, some of which are
     * temporary such as network failures. For temporary failures, JanusGraph will re-attempt to persist the
     * state up to the number of times specified.
     */
//    public static final ConfigOption<Integer> WRITE_ATTEMPTS = new ConfigOption<>(STORAGE_NS,"write-attempts",
//            "Number of attempts for write operations that might experience temporary failures",
//            ConfigOption.Type.MASKABLE, 5, ConfigOption.positiveInt());

    /*
     * Number of times the database attempts to execute a read operation against the storage layer in the current transaction.
     * A read operation might fail for various reasons, some of which are
     * temporary such as network failures. For temporary failures, JanusGraph will re-attempt to read the
     * state up to the number of times specified before failing the transaction
     */
//    public static final ConfigOption<Integer> READ_ATTEMPTS = new ConfigOption<>(STORAGE_NS,"read-attempts",
//            "Number of attempts for read operations that might experience temporary failures",
//            ConfigOption.Type.MASKABLE, 3, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> STORAGE_WRITE_WAITTIME = new ConfigOption<>(STORAGE_NS,"write-time",
            "Maximum time (in ms) to wait for a backend write operation to complete successfully. If a backend write operation" +
            "fails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted. ",
            ConfigOption.Type.MASKABLE, Duration.ofSeconds(100L));

    public static final ConfigOption<Duration> STORAGE_READ_WAITTIME = new ConfigOption<>(STORAGE_NS,"read-time",
            "Maximum time (in ms) to wait for a backend read operation to complete successfully. If a backend read operation" +
                    "fails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted. ",
            ConfigOption.Type.MASKABLE, Duration.ofSeconds(10L));

    /**
     * If enabled, JanusGraph attempts to parallelize storage operations against the storage backend using a fixed thread pool shared
     * across the entire JanusGraph graph database instance. Parallelization is only applicable to certain storage operations and
     * can be beneficial when the operation is I/O bound.
     */
    public static final ConfigOption<Boolean> PARALLEL_BACKEND_OPS = new ConfigOption<>(STORAGE_NS,"parallel-backend-ops",
            "Whether JanusGraph should attempt to parallelize storage operations",
            ConfigOption.Type.MASKABLE, true);

    /**
     * A unique identifier for the machine running the JanusGraph instance.
     * It must be ensured that no other machine accessing the storage backend can have the same identifier.
     */
//    public static final ConfigOption<String> INSTANCE_RID_RAW = new ConfigOption<>(STORAGE_NS,"machine-id",
//            "A unique identifier for the machine running the JanusGraph instance",
//            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String[]> STORAGE_HOSTS = new ConfigOption<>(STORAGE_NS,"hostname",
            "The hostname or comma-separated list of hostnames of storage backend servers.  " +
            "This is only applicable to some storage backends, such as cassandra and hbase.",
            ConfigOption.Type.LOCAL, new String[]{NetworkUtil.getLoopbackAddress()});

    /**
     * Configuration key for the port on which to connect to remote storage backend servers.
     */
    public static final ConfigOption<Integer> STORAGE_PORT = new ConfigOption<>(STORAGE_NS,"port",
            "The port on which to connect to storage backend servers. For HBase, it is the Zookeeper port.",
            ConfigOption.Type.LOCAL, Integer.class);

    /**
     * Username and password keys to be used to specify an access credential that may be needed to connect
     * with a secured storage backend.
     */
    public static final ConfigOption<String> AUTH_USERNAME = new ConfigOption<>(STORAGE_NS,"username",
            "Username to authenticate against backend",
            ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> AUTH_PASSWORD = new ConfigOption<>(STORAGE_NS,"password",
            "Password to authenticate against backend",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Default timeout when connecting to a remote database instance
     * <p>
     */
    public static final ConfigOption<Duration> CONNECTION_TIMEOUT = new ConfigOption<>(STORAGE_NS,"connection-timeout",
            "Default timeout, in milliseconds, when connecting to a remote database instance",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(10000L));

    /**
     * Time in milliseconds for backend manager to wait for the storage backends to
     * become available when JanusGraph is run in server mode. Should the backend manager
     * experience exceptions when attempting to access the storage backend it will retry
     * until this timeout is exceeded.
     * <p>
     * A wait time of 0 disables waiting.
     * <p>
     */
    public static final ConfigOption<Duration> SETUP_WAITTIME = new ConfigOption<>(STORAGE_NS,"setup-wait",
            "Time in milliseconds for backend manager to wait for the storage backends to become available when JanusGraph is run in server mode",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(60000L));

    /**
     * Default number of results to pull over the wire when iterating over a distributed
     * storage backend.
     * This is batch size of results to pull when iterating a result set.
     */
    public static final ConfigOption<Integer> PAGE_SIZE = new ConfigOption<>(STORAGE_NS,"page-size",
            "JanusGraph break requests that may return many results from distributed storage backends " +
            "into a series of requests for small chunks/pages of results, where each chunk contains " +
            "up to this many elements.",
            ConfigOption.Type.MASKABLE, 100);

    public static final ConfigOption<Boolean> DROP_ON_CLEAR = new ConfigOption<>(STORAGE_NS, "drop-on-clear",
        "Whether to drop the graph database (true) or delete rows (false) when clearing storage. " +
            "Note that some backends always drop the graph database when clearing storage. Also note that indices are " +
            "always dropped when clearing storage.",
        ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace LOCK_NS =
            new ConfigNamespace(STORAGE_NS, "lock", "Options for locking on eventually-consistent stores");

    /**
     * Number of times the system attempts to acquire a lock before giving up and throwing an exception.
     */
    public static final ConfigOption<Integer> LOCK_RETRY = new ConfigOption<>(LOCK_NS, "retries",
            "Number of times the system attempts to acquire a lock before giving up and throwing an exception",
            ConfigOption.Type.MASKABLE, 3);

    /**
     * The number of milliseconds the system waits for a lock application to be acknowledged by the storage backend.
     * Also, the time waited at the end of all lock applications before verifying that the applications were successful.
     * This value should be a small multiple of the average consistent write time.
     */
    public static final ConfigOption<Duration> LOCK_WAIT = new ConfigOption<>(LOCK_NS, "wait-time",
            "Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend. " +
            "Also, the time waited at the end of all lock applications before verifying that the applications were successful. " +
            "This value should be a small multiple of the average consistent write time.",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.ofMillis(100L));

    /**
     * Number of milliseconds after which a lock is considered to have expired. Lock applications that were not released
     * are considered expired after this time and released.
     * This value should be larger than the maximum time a transaction can take in order to guarantee that no correctly
     * held applications are expired pre-maturely and as small as possible to avoid dead lock.
     */
    public static final ConfigOption<Duration> LOCK_EXPIRE = new ConfigOption<>(LOCK_NS, "expiry-time",
            "Number of milliseconds after which a lock is considered to have expired. " +
            "Lock applications that were not released are considered expired after this time and released. " +
            "This value should be larger than the maximum time a transaction can take in order to guarantee " +
            "that no correctly held applications are expired pre-maturely and as small as possible to avoid dead lock.",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.ofMillis(300 * 1000L));

    /**
     * Whether to attempt to delete expired locks from the storage backend. True
     * will attempt to delete expired locks in a background daemon thread. False
     * will never attempt to delete expired locks. This option is only
     * meaningful for the default lock backend.
     *
     * @see #LOCK_BACKEND
     */
    public static final ConfigOption<Boolean> LOCK_CLEAN_EXPIRED = new ConfigOption<>(LOCK_NS, "clean-expired",
            "Whether to delete expired locks from the storage backend",
            ConfigOption.Type.MASKABLE, false);

    /**
     * Locker type to use.  The supported types are in {@link org.janusgraph.diskstorage.Backend}.
     */
    public static final ConfigOption<String> LOCK_BACKEND = new ConfigOption<>(LOCK_NS, "backend",
            "Locker type to use",
            ConfigOption.Type.GLOBAL_OFFLINE, "consistentkey");

    /**
     * Configuration setting key for the local lock mediator prefix
     */
    public static final ConfigOption<String> LOCK_LOCAL_MEDIATOR_GROUP =
            new ConfigOption<>(LOCK_NS, "local-mediator-group",
            "This option determines the LocalLockMediator instance used for early detection of lock contention " +
            "between concurrent JanusGraph graph instances within the same process which are connected to the same " +
            "storage backend.  JanusGraph instances that have the same value for this variable will attempt to discover " +
            "lock contention among themselves in memory before proceeding with the general-case distributed locking " +
            "code.  JanusGraph generates an appropriate default value for this option at startup.  Overridding " +
            "the default is generally only useful in testing.", ConfigOption.Type.LOCAL, String.class);


    // ################ STORAGE - META #######################

    public static final ConfigNamespace STORE_META_NS = new ConfigNamespace(STORAGE_NS,"meta","Meta data to include in storage backend retrievals",true);

    public static final ConfigOption<Boolean> STORE_META_TIMESTAMPS = new ConfigOption<>(STORE_META_NS,"timestamps",
            "Whether to include timestamps in retrieved entries for storage backends that automatically annotated entries with timestamps",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Boolean> STORE_META_TTL = new ConfigOption<>(STORE_META_NS,"ttl",
            "Whether to include ttl in retrieved entries for storage backends that support storage and retrieval of cell level TTL",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Boolean> STORE_META_VISIBILITY = new ConfigOption<>(STORE_META_NS,"visibility",
            "Whether to include visibility in retrieved entries for storage backends that support cell level visibility",
            ConfigOption.Type.GLOBAL, true);


    // ################ CLUSTERING ###########################
    // ################################################

    public static final ConfigNamespace CLUSTER_NS = new ConfigNamespace(ROOT_NS,"cluster","Configuration options for multi-machine deployments");

    /*
     * Whether the id space should be partitioned for equal distribution of keys. If the keyspace is ordered, this needs to be
     * enabled to ensure an even distribution of data. If the keyspace is random/hashed, then enabling this only has the benefit
     * of de-congesting a single id pool in the database.
     */
//    public static final ConfigOption<Boolean> CLUSTER_PARTITION = new ConfigOption<Boolean>(CLUSTER_NS,"partition",
//            "Whether the graph's element should be randomly distributed across the cluster " +
//            "(true) or explicitly allocated to individual partition blocks based on the configured graph partitioner (false). " +
//            "Unless explicitly set, this defaults false for stores that hash keys and defaults true for stores that preserve key order " +
//            "(such as HBase and Cassandra with ByteOrderedPartitioner).",
//            ConfigOption.Type.FIXED, false);


    public static final ConfigOption<Integer> CLUSTER_MAX_PARTITIONS = new ConfigOption<>(CLUSTER_NS,"max-partitions",
            "The number of virtual partition blocks created in the partitioned graph. This should be larger than the maximum expected number of nodes" +
                    " in the JanusGraph graph cluster. Must be greater than 1 and a power of 2.",
            ConfigOption.Type.FIXED, 32, integer -> integer!=null && integer>1 && NumberUtil.isPowerOf2(integer));



    // ################ IDS ###########################
    // ################################################

    public static final ConfigNamespace IDS_NS = new ConfigNamespace(ROOT_NS,"ids","General configuration options for graph element IDs");

    /**
     * Size of the block to be acquired. Larger block sizes require fewer block applications but also leave a larger
     * fraction of the id pool occupied and potentially lost. For write heavy applications, larger block sizes should
     * be chosen.
     */
    public static final ConfigOption<Integer> IDS_BLOCK_SIZE = new ConfigOption<>(IDS_NS,"block-size",
            "Globally reserve graph element IDs in chunks of this size.  Setting this too low will make commits " +
            "frequently block on slow reservation requests.  Setting it too high will result in IDs wasted when a " +
            "graph instance shuts down with reserved but mostly-unused blocks.",
            ConfigOption.Type.GLOBAL_OFFLINE, 10000);

    /**
     * The name of the ID store. Currently this defaults to janusgraph_ids. You can override the ID store to
     * facilitate migration from JanusGraph's predecessor, Titan. Previously, this KCVStore was named titan_ids.
     */
    public static final ConfigOption<String> IDS_STORE_NAME = new ConfigOption<>(IDS_NS, "store-name",
        "The name of the ID KCVStore. IDS_STORE_NAME is meant to be used only for backward compatibility with Titan, " +
            "and should not be used explicitly in normal operations or in new graphs.",
        ConfigOption.Type.GLOBAL_OFFLINE, JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME);


    /**
     * If flush ids is enabled, vertices and edges are assigned ids immediately upon creation. If not, then ids are only
     * assigned when the transaction is committed.
     */
    public static final ConfigOption<Boolean> IDS_FLUSH = new ConfigOption<>(IDS_NS,"flush",
            "When true, vertices and edges are assigned IDs immediately upon creation.  When false, " +
            "IDs are assigned only when the transaction commits.",
            ConfigOption.Type.MASKABLE, true);

    /**
     * The number of milliseconds that the JanusGraph id pool manager will wait before giving up on allocating a new block
     * of ids. Note, that failure to allocate a new id block will cause the entire database to fail, hence this value
     * should be set conservatively. Choose a high value if there is a lot of contention around id allocation.
     */
    public static final ConfigOption<Duration> IDS_RENEW_TIMEOUT = new ConfigOption<>(IDS_NS,"renew-timeout",
            "The number of milliseconds that the JanusGraph id pool manager will wait before giving up on allocating a new block of ids",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(120000L));

    /**
     * Configures when the id pool manager will attempt to allocate a new id block. When all but the configured percentage
     * of the current block is consumed, a new block will be allocated. Larger values should be used if a lot of ids
     * are allocated in a short amount of time. Value must be in (0,1].
     */
    public static final ConfigOption<Double> IDS_RENEW_BUFFER_PERCENTAGE = new ConfigOption<>(IDS_NS,"renew-percentage",
            "When the most-recently-reserved ID block has only this percentage of its total IDs remaining " +
            "(expressed as a value between 0 and 1), JanusGraph asynchronously begins reserving another block. " +
            "This helps avoid transaction commits waiting on ID reservation even if the block size is relatively small.",
            ConfigOption.Type.MASKABLE, 0.3);

    // ################ IDAUTHORITY ###################
    // ################################################

    public static final ConfigNamespace IDAUTHORITY_NS = new ConfigNamespace(IDS_NS,"authority","Configuration options for graph element ID reservation/allocation");

    /**
     * The number of milliseconds the system waits for an id block application to be acknowledged by the storage backend.
     * Also, the time waited after the application before verifying that the application was successful.
     */
    public static final ConfigOption<Duration> IDAUTHORITY_WAIT = new ConfigOption<>(IDAUTHORITY_NS,"wait-time",
            "The number of milliseconds the system waits for an ID block reservation to be acknowledged by the storage backend",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.ofMillis(300L));

    /**
     * Sets the strategy used by {@link ConsistentKeyIDAuthority} to avoid
     * contention in ID block allocation between JanusGraph instances concurrently
     * sharing a single distributed storage backend.
     */
    // This is set to GLOBAL_OFFLINE as opposed to MASKABLE or GLOBAL to prevent mixing both global-randomized and local-manual modes within the same cluster
    public static final ConfigOption<ConflictAvoidanceMode> IDAUTHORITY_CONFLICT_AVOIDANCE = new ConfigOption<>(IDAUTHORITY_NS,"conflict-avoidance-mode",
            "This setting helps separate JanusGraph instances sharing a single graph storage backend avoid contention when reserving ID blocks, " +
            "increasing overall throughput.",
            ConfigOption.Type.GLOBAL_OFFLINE, ConflictAvoidanceMode.class, ConflictAvoidanceMode.NONE);

    /**
     * When JanusGraph allocates IDs with {@link org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode#GLOBAL_AUTO}
     * configured, it picks a random unique ID marker and attempts to allocate IDs
     * from a partition using the marker. The ID markers function as
     * subpartitions with each ID partition. If the attempt fails because that
     * partition + unique id combination is already completely allocated, then
     * JanusGraph will generate a new random unique ID and try again. This controls
     * the maximum number of attempts before JanusGraph assumes the entire partition
     * is allocated and fails the request. It must be set to at least 1 and
     * should generally be set to 3 or more.
     * <p>
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is not configured to
     * {@link org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode#GLOBAL_AUTO}.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_RETRIES = new ConfigOption<>(IDAUTHORITY_NS,"randomized-conflict-avoidance-retries",
            "Number of times the system attempts ID block reservations with random conflict avoidance tags before giving up and throwing an exception",
            ConfigOption.Type.MASKABLE, 5);

    /**
     * Configures the number of bits of JanusGraph assigned ids that are reserved for a unique id marker that
     * allows the id allocation to be scaled over multiple sub-clusters and to reduce race-conditions
     * when a lot of JanusGraph instances attempt to allocate ids at the same time (e.g. during parallel bulk loading)
     *
     * IMPORTANT: This should never ever, ever be modified from its initial value and ALL JanusGraph instances must use the
     * same value. Otherwise, data corruption will occur.
     *
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is configured to
     * {@link org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode#NONE}. However, note that while the
     * conflict avoidance mode can be changed, this setting cannot ever be changed and must therefore be considered a priori.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_BITS = new ConfigOption<>(IDAUTHORITY_NS,"conflict-avoidance-tag-bits",
            "Configures the number of bits of JanusGraph-assigned element IDs that are reserved for the conflict avoidance tag",
            ConfigOption.Type.FIXED, 4 , uniqueIdBitWidth -> uniqueIdBitWidth>=0 && uniqueIdBitWidth<=16);

    /**
     * Unique id marker to be used by this JanusGraph instance when allocating ids. The unique id marker
     * must be non-negative and fit within the number of unique id bits configured.
     * By assigning different unique id markers to individual JanusGraph instances it can be assured
     * that those instances don't conflict with one another when attempting to allocate new id blocks.
     *
     * IMPORTANT: The configured unique id marker must fit within the configured unique id bit width.
     *
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is configured to
     * {@link org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode#NONE}.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_TAG = new ConfigOption<>(IDAUTHORITY_NS,"conflict-avoidance-tag",
            "Conflict avoidance tag to be used by this JanusGraph instance when allocating IDs",
            ConfigOption.Type.LOCAL, 0);


    // ############## External Index ######################
    // ################################################

    public static final ConfigNamespace INDEX_NS = new ConfigNamespace(ROOT_NS,"index","Configuration options for the individual indexing backends",true);


    /**
     * Define the indexing backed to use for index support
     */
    public static final ConfigOption<String> INDEX_BACKEND = new ConfigOption<>(INDEX_NS,"backend",
            "The indexing backend used to extend and optimize JanusGraph's query functionality. " +
            "This setting is optional.  JanusGraph can use multiple heterogeneous index backends.  " +
            "Hence, this option can appear more than once, so long as the user-defined name between " +
            "\"" + INDEX_NS.getName() + "\" and \"backend\" is unique among appearances." +
            "Similar to the storage backend, this should be set to one of " +
            "JanusGraph's built-in shorthand names for its standard index backends " +
            "(shorthands: " + Joiner.on(", ").join(StandardIndexProvider.getAllShorthands()) + ") " +
            "or to the full package and classname of a custom/third-party IndexProvider implementation.",
            ConfigOption.Type.GLOBAL_OFFLINE, "elasticsearch");

    public static final ConfigOption<String> INDEX_DIRECTORY = new ConfigOption<>(INDEX_NS,"directory",
            "Directory to store index data locally",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigOption<String> INDEX_NAME = new ConfigOption<>(INDEX_NS,"index-name",
            "Name of the index if required by the indexing backend",
            ConfigOption.Type.GLOBAL_OFFLINE, "janusgraph");

    public static final ConfigOption<String[]> INDEX_HOSTS = new ConfigOption<>(INDEX_NS,"hostname",
            "The hostname or comma-separated list of hostnames of index backend servers.  " +
            "This is only applicable to some index backends, such as elasticsearch and solr.",
            ConfigOption.Type.MASKABLE, new String[]{NetworkUtil.getLoopbackAddress()});

    public static final ConfigOption<Integer> INDEX_PORT = new ConfigOption<>(INDEX_NS,"port",
            "The port on which to connect to index backend servers",
            ConfigOption.Type.MASKABLE, Integer.class);

    public static final ConfigOption<String> INDEX_CONF_FILE = new ConfigOption<>(INDEX_NS,"conf-file",
            "Path to a configuration file for those indexing backends that require/support a separate config file",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigOption<Integer> INDEX_MAX_RESULT_SET_SIZE = new ConfigOption<>(INDEX_NS, "max-result-set-size",
            "Maximum number of results to return if no limit is specified. For index backends that support scrolling, it represents " +
                    "the number of results in each batch",
            ConfigOption.Type.MASKABLE, 50);

    public static final ConfigOption<Boolean> INDEX_NAME_MAPPING = new ConfigOption<>(INDEX_NS,"map-name",
            "Whether to use the name of the property key as the field name in the index. It must be ensured, that the" +
                    "indexed property key names are valid field names. Renaming the property key will NOT rename the field " +
                    "and its the developers responsibility to avoid field collisions.",
            ConfigOption.Type.GLOBAL, true);


    // ############## Logging System ######################
    // ################################################

    public static final ConfigNamespace LOG_NS = new ConfigNamespace(GraphDatabaseConfiguration.ROOT_NS,"log","Configuration options for JanusGraph's logging system",true);

    public static final String MANAGEMENT_LOG = "janusgraph";
    public static final String TRANSACTION_LOG = "tx";
    public static final String USER_LOG = "user";
    public static final String USER_LOG_PREFIX = "ulog_";

    public static final Duration TRANSACTION_LOG_DEFAULT_TTL = Duration.ofDays(7);

    public static final ConfigOption<String> LOG_BACKEND = new ConfigOption<>(LOG_NS,"backend",
            "Define the log backed to use",
            ConfigOption.Type.GLOBAL_OFFLINE, "default");

    public static final ConfigOption<Integer> LOG_NUM_BUCKETS = new ConfigOption<>(LOG_NS,"num-buckets",
            "The number of buckets to split log entries into for load balancing",
            ConfigOption.Type.GLOBAL_OFFLINE, 1, ConfigOption.positiveInt());

    public static final ConfigOption<Integer> LOG_SEND_BATCH_SIZE = new ConfigOption<>(LOG_NS,"send-batch-size",
            "Maximum number of log messages to batch up for sending for logging implementations that support batch sending",
            ConfigOption.Type.MASKABLE, 256, ConfigOption.positiveInt());

    public static final ConfigOption<Integer> LOG_READ_BATCH_SIZE = new ConfigOption<>(LOG_NS,"read-batch-size",
            "Maximum number of log messages to read at a time for logging implementations that read messages in batches",
            ConfigOption.Type.MASKABLE, 1024, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> LOG_SEND_DELAY = new ConfigOption<>(LOG_NS,"send-delay",
            "Maximum time in ms that messages can be buffered locally before sending in batch",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(1000L));

    public static final ConfigOption<Duration> LOG_READ_INTERVAL = new ConfigOption<>(LOG_NS,"read-interval",
            "Time in ms between message readings from the backend for this logging implementations that read message in batch",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(5000L));

    public static final ConfigOption<Integer> LOG_READ_THREADS = new ConfigOption<>(LOG_NS,"read-threads",
            "Number of threads to be used in reading and processing log messages",
            ConfigOption.Type.MASKABLE, 1, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> LOG_STORE_TTL = new ConfigOption<Duration>(LOG_NS,"ttl",
            "Sets a TTL on all log entries, meaning" +
                    "that all entries added to this log expire after the configured amount of time. Requires" +
                    "that the log implementation supports TTL.",
            ConfigOption.Type.GLOBAL, Duration.class, sd -> null != sd && !sd.isZero());

    // ############## Attributes ######################
    // ################################################

    public static final ConfigNamespace ATTRIBUTE_NS = new ConfigNamespace(ROOT_NS,"attributes","Configuration options for attribute handling");

    public static final ConfigNamespace CUSTOM_ATTRIBUTE_NS = new ConfigNamespace(ATTRIBUTE_NS,"custom","Custom attribute serialization and handling",true);

    public static final String ATTRIBUTE_PREFIX = "attribute";

    public static final ConfigOption<String> CUSTOM_ATTRIBUTE_CLASS = new ConfigOption<>(CUSTOM_ATTRIBUTE_NS,"attribute-class",
            "Class of the custom attribute to be registered",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);
    public static final ConfigOption<String> CUSTOM_SERIALIZER_CLASS = new ConfigOption<>(CUSTOM_ATTRIBUTE_NS,"serializer-class",
            "Class of the custom attribute serializer to be registered",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);

    // ################ Metrics #######################
    // ################################################

    /**
     * Configuration key prefix for Metrics.
     */
    public static final ConfigNamespace METRICS_NS = new ConfigNamespace(ROOT_NS,"metrics","Configuration options for metrics reporting");

    /**
     * Whether to enable basic timing and operation count monitoring on backend
     * methods using the {@code com.codahale.metrics} package.
     */
    public static final ConfigOption<Boolean> BASIC_METRICS = new ConfigOption<>(METRICS_NS,"enabled",
            "Whether to enable basic timing and operation count monitoring on backend",
            ConfigOption.Type.MASKABLE, false);

    /**
     * This is the prefix used outside of a graph database configuration, or for
     * operations where a system-internal transaction is necessary as an
     * implementation detail. It currently can't be modified, though there is no
     * substantial technical obstacle preventing it from being configured --
     * some kind of configuration object is in scope everywhere it is used, and
     * it could theoretically be stored in and read from that object.
     */
    public static final String METRICS_PREFIX_DEFAULT = "org.janusgraph";
    public static final String METRICS_SYSTEM_PREFIX_DEFAULT = METRICS_PREFIX_DEFAULT + "." + "sys";
    public static final String METRICS_SCHEMA_PREFIX_DEFAULT = METRICS_SYSTEM_PREFIX_DEFAULT + "." + "schema";

    /**
     * The default name prefix for Metrics reported by JanusGraph. All metric names
     * will begin with this string and a period. This value can be overridden on
     * a transaction-specific basis through
     * {@link StandardTransactionBuilder#groupName(String)}.
     * <p>
     * Default = {@literal #METRICS_PREFIX_DEFAULT}
     */
    public static final ConfigOption<String> METRICS_PREFIX = new ConfigOption<>(METRICS_NS,"prefix",
            "The default name prefix for Metrics reported by JanusGraph.",
            ConfigOption.Type.MASKABLE, METRICS_PREFIX_DEFAULT);

    /**
     * Whether to aggregate measurements for the edge store, vertex index, edge
     * index, and ID store.
     * <p>
     * If true, then metrics for each of these backends will use the same metric
     * name ("stores"). All of their measurements will be combined. This setting
     * measures the sum of JanusGraph's backend activity without distinguishing
     * between contributions of its various internal stores.
     * <p>
     * If false, then metrics for each of these backends will use a unique
     * metric name ("idStore", "edgeStore", "vertexIndex", and "edgeIndex").
     * This setting exposes the activity associated with each backend component,
     * but it also multiplies the number of measurements involved by four.
     * <p>
     * This option has no effect when {@link #BASIC_METRICS} is false.
     */
    public static final ConfigOption<Boolean> METRICS_MERGE_STORES = new ConfigOption<>(METRICS_NS,"merge-stores",
            "Whether to aggregate measurements for the edge store, vertex index, edge index, and ID store",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace METRICS_CONSOLE_NS = new ConfigNamespace(METRICS_NS,"console","Configuration options for metrics reporting to console");


    /**
     * Metrics console reporter interval in milliseconds. Leaving this
     * configuration key absent or null disables the console reporter.
     */
    public static final ConfigOption<Duration> METRICS_CONSOLE_INTERVAL = new ConfigOption<>(METRICS_CONSOLE_NS,"interval",
            "Time between Metrics reports printing to the console, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);

    public static final ConfigNamespace METRICS_CSV_NS = new ConfigNamespace(METRICS_NS,"csv","Configuration options for metrics reporting to CSV file");

    /**
     * Metrics CSV reporter interval in milliseconds. Leaving this configuration
     * key absent or null disables the CSV reporter.
     */
    public static final ConfigOption<Duration> METRICS_CSV_INTERVAL = new ConfigOption<>(METRICS_CSV_NS,"interval",
            "Time between dumps of CSV files containing Metrics data, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);

    /**
     * Metrics CSV output directory. It will be created if it doesn't already
     * exist. This option must be non-null if {@link #METRICS_CSV_INTERVAL} is
     * non-null. This option has no effect if {@code #METRICS_CSV_INTERVAL} is
     * null.
     */
    public static final ConfigOption<String> METRICS_CSV_DIR = new ConfigOption<>(METRICS_CSV_NS,"directory",
            "Metrics CSV output directory",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigNamespace METRICS_JMX_NS = new ConfigNamespace(METRICS_NS,"jmx","Configuration options for metrics reporting through JMX");

    /**
     * Whether to report Metrics through a JMX MBean.
     */
    public static final ConfigOption<Boolean> METRICS_JMX_ENABLED = new ConfigOption<>(METRICS_JMX_NS,"enabled",
            "Whether to report Metrics through a JMX MBean",
            ConfigOption.Type.MASKABLE, false);

    /**
     * The JMX domain in which to report Metrics. If null, then Metrics applies
     * its default value.
     */
    public static final ConfigOption<String> METRICS_JMX_DOMAIN = new ConfigOption<>(METRICS_JMX_NS,"domain",
            "The JMX domain in which to report Metrics",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The JMX agentId through which to report Metrics. Calling
     * {@link MBeanServerFactory#findMBeanServer(String)} on this value must
     * return exactly one {@code MBeanServer} at runtime. If null, then Metrics
     * applies its default value.
     */
    public static final ConfigOption<String> METRICS_JMX_AGENTID = new ConfigOption<>(METRICS_JMX_NS,"agentid",
            "The JMX agentId used by Metrics",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigNamespace METRICS_SLF4J_NS = new ConfigNamespace(METRICS_NS,"slf4j","Configuration options for metrics reporting through slf4j");

    /**
     * Metrics Slf4j reporter interval in milliseconds. Leaving this
     * configuration key absent or null disables the Slf4j reporter.
     */
    public static final ConfigOption<Duration> METRICS_SLF4J_INTERVAL = new ConfigOption<>(METRICS_SLF4J_NS,"interval",
            "Time between slf4j logging reports of Metrics data, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);

    /**
     * The complete name of the Logger through which Metrics will report via
     * Slf4j. If non-null, then Metrics will be dumped on
     * {@link LoggerFactory#getLogger(String)} with the configured value as the
     * argument. If null, then Metrics will use its default Slf4j logger.
     */
    public static final ConfigOption<String> METRICS_SLF4J_LOGGER = new ConfigOption<>(METRICS_SLF4J_NS,"logger",
            "The complete name of the Logger through which Metrics will report via Slf4j",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The configuration namespace within {@link #METRICS_NS} for Ganglia.
     */
    public static final ConfigNamespace METRICS_GANGLIA_NS = new ConfigNamespace(METRICS_NS,"ganglia","Configuration options for metrics reporting through Ganglia");

    /**
     * The unicast host or multicast group name to which Metrics will send
     * Ganglia data. Setting this config key has no effect unless
     * {@link #GANGLIA_INTERVAL} is also set.
     */
    public static final ConfigOption<String> GANGLIA_HOST_OR_GROUP = new ConfigOption<>(METRICS_GANGLIA_NS,"hostname",
            "The unicast host or multicast group name to which Metrics will send Ganglia data",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The number of milliseconds to wait between sending Metrics data to the
     * host or group specified by {@link #GANGLIA_HOST_OR_GROUP}. This has no
     * effect unless {@link #GANGLIA_HOST_OR_GROUP} is also set.
     */
    public static final ConfigOption<Duration> GANGLIA_INTERVAL = new ConfigOption<>(METRICS_GANGLIA_NS,"interval",
            "The number of milliseconds to wait between sending Metrics data to Ganglia",
            ConfigOption.Type.MASKABLE, Duration.class);

    /**
     * The port to which Ganglia data are sent.
     * <p>
     */
    public static final ConfigOption<Integer> GANGLIA_PORT = new ConfigOption<>(METRICS_GANGLIA_NS,"port",
            "The port to which Ganglia data are sent",
            ConfigOption.Type.MASKABLE, 8649);

    /**
     * Whether to interpret {@link #GANGLIA_HOST_OR_GROUP} as a unicast or
     * multicast address. If present, it must be either the string "multicast"
     * or the string "unicast".
     * <p>
     */
    public static final ConfigOption<String> GANGLIA_ADDRESSING_MODE = new ConfigOption<>(METRICS_GANGLIA_NS,"addressing-mode",
            "Whether to communicate to Ganglia via uni- or multicast",
            ConfigOption.Type.MASKABLE, "unicast", s -> s!=null && s.equalsIgnoreCase("unicast") || s.equalsIgnoreCase("multicast"));

    /**
     * The multicast TTL to set on outgoing Ganglia datagrams. This has no
     * effect when {@link #GANGLIA_ADDRESSING_MODE} is set to "multicast".
     * <p>
     * This is a TTL in the multicast protocol sense (number of routed hops),
     * not a timestamp sense.
     */
    public static final ConfigOption<Integer> GANGLIA_TTL = new ConfigOption<>(METRICS_GANGLIA_NS,"ttl",
            "The multicast TTL to set on outgoing Ganglia datagrams",
            ConfigOption.Type.MASKABLE, 1);

    /**
     * Whether to send data to Ganglia in the 3.1 protocol format (true) or the
     * 3.0 protocol format (false).
     * <p>
     */
    public static final ConfigOption<Boolean> GANGLIA_USE_PROTOCOL_31 = new ConfigOption<>(METRICS_GANGLIA_NS,"protocol-31",
            "Whether to send data to Ganglia in the 3.1 protocol format",
            ConfigOption.Type.MASKABLE, true);

    /**
     * The host UUID to set on outgoing Ganglia datagrams. If null, no UUID is
     * set on outgoing data.
     * <p>
     * See https://github.com/ganglia/monitor-core/wiki/UUIDSources
     * <p>
     */
    public static final ConfigOption<String> GANGLIA_UUID = new ConfigOption<>(METRICS_GANGLIA_NS,"uuid",
            "The host UUID to set on outgoing Ganglia datagrams. " +
            "See https://github.com/ganglia/monitor-core/wiki/UUIDSources for information about this setting.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * If non-null, it must be a valid Gmetric spoof string formatted as an
     * IP:hostname pair. If null, Ganglia will automatically determine the IP
     * and hostname to set on outgoing datagrams.
     * <p>
     * See https://github.com/ganglia/monitor-core/wiki/Gmetric-Spoofing
     * <p>
     */
    public static final ConfigOption<String> GANGLIA_SPOOF = new ConfigOption<String>(METRICS_GANGLIA_NS,"spoof",
            "If non-null, it must be a valid Gmetric spoof string formatted as an IP:hostname pair. " +
            "See https://github.com/ganglia/monitor-core/wiki/Gmetric-Spoofing for information about this setting.",
            ConfigOption.Type.MASKABLE, String.class, s -> s!=null && 0 < s.indexOf(':'));

    /**
     * The configuration namespace within {@link #METRICS_NS} for
     * Graphite.
     */
    public static final ConfigNamespace METRICS_GRAPHITE_NS = new ConfigNamespace(METRICS_NS,"graphite","Configuration options for metrics reporting through Graphite");

    /**
     * The hostname to receive Graphite plaintext protocol metric data. Setting
     * this config key has no effect unless {@link #GRAPHITE_INTERVAL} is also
     * set.
     */
    public static final ConfigOption<String> GRAPHITE_HOST = new ConfigOption<>(METRICS_GRAPHITE_NS,"hostname",
            "The hostname to receive Graphite plaintext protocol metric data",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The number of milliseconds to wait between sending Metrics data to the
     * host specified {@link #GRAPHITE_HOST}. This has no effect unless
     * {@link #GRAPHITE_HOST} is also set.
     */
    public static final ConfigOption<Duration> GRAPHITE_INTERVAL = new ConfigOption<>(METRICS_GRAPHITE_NS,"interval",
            "The number of milliseconds to wait between sending Metrics data",
            ConfigOption.Type.MASKABLE, Duration.class);

    /**
     * The port to which Graphite data are sent.
     * <p>
     */
    public static final ConfigOption<Integer> GRAPHITE_PORT = new ConfigOption<>(METRICS_GRAPHITE_NS,"port",
            "The port to which Graphite data are sent",
            ConfigOption.Type.MASKABLE, 2003);

    /**
     * A Graphite-specific prefix for reported metrics. If non-null, Metrics
     * prepends this and a "." to all metric names before reporting them to
     * Graphite.
     * <p>
     */
    public static final ConfigOption<String> GRAPHITE_PREFIX = new ConfigOption<>(METRICS_GRAPHITE_NS,"prefix",
            "A Graphite-specific prefix for reported metrics",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigNamespace GREMLIN_NS = new ConfigNamespace(ROOT_NS,"gremlin",
            "Gremlin configuration options");

    public static final ConfigOption<String> GREMLIN_GRAPH = new ConfigOption<>(GREMLIN_NS, "graph",
            "The implementation of graph factory that will be used by gremlin server", ConfigOption.Type.LOCAL, "org.janusgraph.core.JanusGraphFactory");

    // ################ Begin Class Definition #######################
    // ###############################################################

    public static final String SYSTEM_PROPERTIES_STORE_NAME = "system_properties";
    public static final String SYSTEM_CONFIGURATION_IDENTIFIER = "configuration";
    public static final String USER_CONFIGURATION_IDENTIFIER = "userconfig";
    private static final String INCOMPATIBLE_VERSION_EXCEPTION = "StorageBackend version is incompatible with current JanusGraph version: storage [%1s] vs. runtime [%2s]";

    private final Configuration configuration;
    private final ReadConfiguration configurationAtOpen;
    private String uniqueGraphId;
    private final ModifiableConfiguration localConfiguration;

    private boolean readOnly;
    private boolean flushIDs;
    private boolean forceIndexUsage;
    private boolean batchLoading;
    private int txVertexCacheSize;
    private int txDirtyVertexSize;
    private DefaultSchemaMaker defaultSchemaMaker;
    private boolean hasDisabledSchemaConstraints;
    private Boolean propertyPrefetching;
    private boolean adjustQueryLimit;
    private Boolean useMultiQuery;
    private boolean allowVertexIdSetting;
    private boolean logTransactions;
    private String metricsPrefix;
    private String unknownIndexKeyName;

    private StoreFeatures storeFeatures = null;

    public GraphDatabaseConfiguration(ReadConfiguration localConfig) {
        Preconditions.checkNotNull(localConfig);

        configurationAtOpen = localConfig;

        BasicConfiguration localBasicConfiguration = new BasicConfiguration(ROOT_NS,localConfig, BasicConfiguration.Restriction.NONE);
        ModifiableConfiguration overwrite = new ModifiableConfiguration(ROOT_NS,new CommonsConfiguration(), BasicConfiguration.Restriction.NONE);

        final KeyColumnValueStoreManager storeManager = Backend.getStorageManager(localBasicConfiguration);

        final StoreFeatures storeFeatures = storeManager.getFeatures();
        KCVSConfiguration keyColumnValueStoreConfiguration=Backend.getStandaloneGlobalConfiguration(storeManager,localBasicConfiguration);
        final ReadConfiguration globalConfig;

        //Copy over local config options
        localConfiguration = new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration(), BasicConfiguration.Restriction.LOCAL);
        localConfiguration.setAll(getLocalSubset(localBasicConfiguration.getAll()));

        //Read out global configuration
        try {
            // If lock prefix is unspecified, specify it now
            if (!localBasicConfiguration.has(LOCK_LOCAL_MEDIATOR_GROUP)) {
                overwrite.set(LOCK_LOCAL_MEDIATOR_GROUP, storeManager.getName());
            }

            //Freeze global configuration if not already frozen!
            ModifiableConfiguration globalWrite = new ModifiableConfiguration(ROOT_NS,keyColumnValueStoreConfiguration, BasicConfiguration.Restriction.GLOBAL);
            if (!globalWrite.isFrozen()) {
                //Copy over global configurations
                globalWrite.setAll(getGlobalSubset(localBasicConfiguration.getAll()));

                //Write JanusGraph version
                Preconditions.checkArgument(!globalWrite.has(INITIAL_JANUSGRAPH_VERSION),"Database has already been initialized but not frozen");
                globalWrite.set(INITIAL_JANUSGRAPH_VERSION,JanusGraphConstants.VERSION);

                /* If the configuration does not explicitly set a timestamp provider and
                 * the storage backend both supports timestamps and has a preference for
                 * a specific timestamp provider, then apply the backend's preference.
                 */
                if (!localBasicConfiguration.has(TIMESTAMP_PROVIDER)) {
                    StoreFeatures f = storeManager.getFeatures();
                    final TimestampProviders backendPreference;
                    if (f.hasTimestamps() && null != (backendPreference = f.getPreferredTimestamps())) {
                        globalWrite.set(TIMESTAMP_PROVIDER, backendPreference);
                        log.info("Set timestamps to {} according to storage backend preference",
                            LoggerUtil.sanitizeAndLaunder(globalWrite.get(TIMESTAMP_PROVIDER)));
                    } else {
                        globalWrite.set(TIMESTAMP_PROVIDER, TIMESTAMP_PROVIDER.getDefaultValue());
                        log.info("Set default timestamp provider {}",
                            LoggerUtil.sanitizeAndLaunder(globalWrite.get(TIMESTAMP_PROVIDER)));
                    }
                } else {
                    log.info("Using configured timestamp provider {}", localBasicConfiguration.get(TIMESTAMP_PROVIDER));
                }

                globalWrite.freezeConfiguration();
            } else {
                try {
                    String version = globalWrite.get(INITIAL_JANUSGRAPH_VERSION);
                    Preconditions.checkArgument(version!=null,"JanusGraph version has not been initialized");
                    if (!JanusGraphConstants.VERSION.equals(version) && !JanusGraphConstants.COMPATIBLE_VERSIONS.contains(version)) {
                        throw new JanusGraphException(String.format(INCOMPATIBLE_VERSION_EXCEPTION, version, JanusGraphConstants.VERSION));
                    }
                } catch (IllegalStateException ise) {
                    checkBackwardCompatibilityWithTitan(globalWrite, localBasicConfiguration, keyColumnValueStoreConfiguration, overwrite);
                }

                final boolean managedOverridesAllowed;

                if (localBasicConfiguration.has(ALLOW_STALE_CONFIG))
                    managedOverridesAllowed = localBasicConfiguration.get(ALLOW_STALE_CONFIG);
                else if (globalWrite.has(ALLOW_STALE_CONFIG))
                    managedOverridesAllowed = globalWrite.get(ALLOW_STALE_CONFIG);
                else
                    managedOverridesAllowed = ALLOW_STALE_CONFIG.getDefaultValue();

                // Check for disagreement between local and backend values for GLOBAL(_OFFLINE) and FIXED options
                // The point of this check is to find edits to the local config which have no effect (and therefore likely indicate misconfiguration)
                Set<String> optionsWithDiscrepancies = Sets.newHashSet();
                for (Map.Entry<ConfigElement.PathIdentifier, Object> ent : getManagedSubset(localBasicConfiguration.getAll()).entrySet()) {
                    ConfigElement.PathIdentifier pid = ent.getKey();
                    assert pid.element.isOption();
                    ConfigOption<?> opt = (ConfigOption<?>)pid.element;
                    Object localValue = ent.getValue();

                    // Get the storage backend's setting and compare with localValue
                    Object storeValue = globalWrite.get(opt, pid.umbrellaElements);

                    // Check if the value is to be overwritten
                    if (overwrite.has(opt, pid.umbrellaElements))
                    {
                    	storeValue = overwrite.get(opt, pid.umbrellaElements);
                    }

                    // Most validation predicate implementations disallow null, but we can't assume that here
                    final boolean match;
                    if (null != localValue && null != storeValue) {
                        match = localValue.equals(storeValue);
                    } else {
                        match = null == localValue && null == storeValue;
                    }

                    // Log each option with value disagreement between local and backend configs
                    if (!match) {
                        String fullOptionName = ConfigElement.getPath(pid.element, pid.umbrellaElements);
                        String template = "Local setting {}={} (Type: {}) is overridden by globally managed value ({}).  Use the {} interface instead of the local configuration to control this setting.";
                        Object replacements[] = new Object[] { fullOptionName, localValue, opt.getType(), storeValue, ManagementSystem.class.getSimpleName() };
                        if (managedOverridesAllowed) { // Lower log severity when this is enabled
                            log.warn(template, replacements);
                        } else {
                            log.error(template, replacements);
                        }
                        optionsWithDiscrepancies.add(fullOptionName);
                    }
                }

                if (0 < optionsWithDiscrepancies.size() && !managedOverridesAllowed) {
                    String template = "Local settings present for one or more globally managed options: [%s].  These options are controlled through the %s interface; local settings have no effect.";
                    throw new JanusGraphConfigurationException(String.format(template, Joiner.on(", ").join(optionsWithDiscrepancies), ManagementSystem.class.getSimpleName()));
                }
            }

            globalConfig = keyColumnValueStoreConfiguration.asReadConfiguration();
        } finally {
            keyColumnValueStoreConfiguration.close();
        }
        Configuration combinedConfig = new MixedConfiguration(ROOT_NS,globalConfig,localConfig);

        //Compute unique instance id
        this.uniqueGraphId = getOrGenerateUniqueInstanceId(combinedConfig);
        overwrite.set(UNIQUE_INSTANCE_ID, this.uniqueGraphId);

        //Default log configuration for system and tx log
        //TRANSACTION LOG: send_delay=0, ttl=2days and backend=default
        Preconditions.checkArgument(combinedConfig.get(LOG_BACKEND,TRANSACTION_LOG).equals(LOG_BACKEND.getDefaultValue()),
                "Must use default log backend for transaction log");
        Preconditions.checkArgument(!combinedConfig.has(LOG_SEND_DELAY,TRANSACTION_LOG) ||
                combinedConfig.get(LOG_SEND_DELAY, TRANSACTION_LOG).isZero(),"Send delay must be 0 for transaction log.");
        overwrite.set(LOG_SEND_DELAY, Duration.ZERO,TRANSACTION_LOG);
        if (!combinedConfig.has(LOG_STORE_TTL,TRANSACTION_LOG) && TTLKCVSManager.supportsAnyTTL(storeFeatures)) {
            overwrite.set(LOG_STORE_TTL,TRANSACTION_LOG_DEFAULT_TTL,TRANSACTION_LOG);
        }
        //SYSTEM MANAGEMENT LOG: backend=default and send_delay=0 and key_consistent=true and fixed-partitions=true
        Preconditions.checkArgument(combinedConfig.get(LOG_BACKEND,MANAGEMENT_LOG).equals(LOG_BACKEND.getDefaultValue()),
                "Must use default log backend for system log");
        Preconditions.checkArgument(!combinedConfig.has(LOG_SEND_DELAY,MANAGEMENT_LOG) ||
                combinedConfig.get(LOG_SEND_DELAY,MANAGEMENT_LOG).isZero(),"Send delay must be 0 for system log.");
        overwrite.set(LOG_SEND_DELAY, Duration.ZERO, MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(KCVSLog.LOG_KEY_CONSISTENT, MANAGEMENT_LOG) ||
                combinedConfig.get(KCVSLog.LOG_KEY_CONSISTENT, MANAGEMENT_LOG), "Management log must be configured to be key-consistent");
        overwrite.set(KCVSLog.LOG_KEY_CONSISTENT,true,MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(KCVSLogManager.LOG_FIXED_PARTITION,MANAGEMENT_LOG)
                || combinedConfig.get(KCVSLogManager.LOG_FIXED_PARTITION,MANAGEMENT_LOG),"Fixed partitions must be enabled for management log");
        overwrite.set(KCVSLogManager.LOG_FIXED_PARTITION,true,MANAGEMENT_LOG);

        this.configuration = new MergedConfiguration(overwrite,combinedConfig);
        preLoadConfiguration();
    }

    private void checkBackwardCompatibilityWithTitan(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration, KCVSConfiguration keyColumnValueStoreConfiguration, ModifiableConfiguration overwrite) {
        String version = globalWrite.get(TITAN_COMPATIBLE_VERSIONS);
        Preconditions.checkArgument(version!=null,"JanusGraph version nor Titan compatibility have not been initialized");
        if (!JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(version)) {
            throw new JanusGraphException(String.format(INCOMPATIBLE_VERSION_EXCEPTION, version, JanusGraphConstants.VERSION));
        }

        // When connecting to a store created by Titan the ID store name will not be in the
        // global configuration as it was not something which was configurable with Titan.
        // So to ensure compatibility override the default to titan_ids.
        boolean localIdStoreIsDefault = JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME.equals(localBasicConfiguration.get(IDS_STORE_NAME));

        boolean usingTitanIdStore = localIdStoreIsDefault || JanusGraphConstants.TITAN_ID_STORE_NAME.equals(localBasicConfiguration.get(IDS_STORE_NAME));
        boolean existingKeyStore = keyColumnValueStoreConfiguration.get(IDS_STORE_NAME.getName(), IDS_STORE_NAME.getDatatype()) != null;

        Preconditions.checkArgument(usingTitanIdStore,"ID store for Titan compatibility has not been initialized to: " + JanusGraphConstants.TITAN_ID_STORE_NAME);
        if (!existingKeyStore) {
            log.info("Setting {} to {} for Titan compatibility", IDS_STORE_NAME.getName(), JanusGraphConstants.TITAN_ID_STORE_NAME);
            overwrite.set(IDS_STORE_NAME, JanusGraphConstants.TITAN_ID_STORE_NAME);
        }
    }

    private static Map<ConfigElement.PathIdentifier, Object> getGlobalSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> {
            assert entry.getKey().element.isOption();
            return ((ConfigOption)entry.getKey().element).isGlobal();
        });
    }

    private static Map<ConfigElement.PathIdentifier, Object> getLocalSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> {
            assert entry.getKey().element.isOption();
            return ((ConfigOption)entry.getKey().element).isLocal();
        });
    }

    private static Map<ConfigElement.PathIdentifier, Object> getManagedSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> {
            assert entry.getKey().element.isOption();
            return ((ConfigOption)entry.getKey().element).isManaged();
        });
    }

    private static final AtomicLong INSTANCE_COUNTER = new AtomicLong(0);

    private static String getSuffix(Configuration config) {
        final String suffix;
        if (config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX)) {
            suffix = LongEncoding.encode(config.get(
                    GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX));
        } else if (!config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)) {
            suffix = ManagementFactory.getRuntimeMXBean().getName() + LongEncoding.encode(INSTANCE_COUNTER.incrementAndGet());
        } else {
            suffix = "";
        }
        return suffix;
    }

    private static String getUid(Configuration config) {
        final String localHostErrMsg = "Cannot determine local host";
        final String uid;
        if (config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)
          && config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)) {
            try {
                uid = Inet4Address.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new JanusGraphConfigurationException(localHostErrMsg, e);
            }
        } else {
            final byte[] addrBytes;
            try {
                addrBytes = Inet4Address.getLocalHost().getAddress();
            } catch (UnknownHostException e) {
                throw new JanusGraphConfigurationException(localHostErrMsg, e);
            }
            uid = new String(Hex.encodeHex(addrBytes));
        }
        return uid;

    }

    private static String computeUniqueInstanceId(Configuration config) {
        final String suffix = getSuffix(config);
        final String uid = getUid(config);
        String instanceId = uid + suffix;
        for (char c : ConfigElement.ILLEGAL_CHARS) {
            instanceId = StringUtils.replaceChars(instanceId,c,'-');
        }
        return instanceId;
    }

    public static String getOrGenerateUniqueInstanceId(Configuration config) {
        String uid;
        if (!config.has(UNIQUE_INSTANCE_ID)) {
            uid = computeUniqueInstanceId(config);
            log.info("Generated {}={}", UNIQUE_INSTANCE_ID.getName(), uid);
        } else {
            uid = config.get(UNIQUE_INSTANCE_ID);
        }
        Preconditions.checkArgument(!StringUtils.containsAny(uid,ConfigElement.ILLEGAL_CHARS),"Invalid unique identifier: %s",uid);
        return uid;
    }

    public static ModifiableConfiguration buildGraphConfiguration() {
        return new ModifiableConfiguration(ROOT_NS,
                new CommonsConfiguration(new BaseConfiguration()),
                BasicConfiguration.Restriction.NONE);
    }

    public static ModifiableConfiguration buildJobConfiguration() {
        return new ModifiableConfiguration(JOB_NS,
                new CommonsConfiguration(new BaseConfiguration()),
                BasicConfiguration.Restriction.NONE);
    }


    public static String getSystemMetricsPrefix() {
        return METRICS_SYSTEM_PREFIX_DEFAULT;
    }

    public static ModifiableConfiguration getGlobalSystemConfig(Backend backend) {
        return new ModifiableConfiguration(ROOT_NS,
                backend.getGlobalSystemConfig(), BasicConfiguration.Restriction.GLOBAL);
    }

    private void preLoadConfiguration() {
        readOnly = configuration.get(STORAGE_READONLY);
        flushIDs = configuration.get(IDS_FLUSH);
        forceIndexUsage = configuration.get(FORCE_INDEX_USAGE);
        batchLoading = configuration.get(STORAGE_BATCH);
        String autoTypeMakerName = configuration.get(AUTO_TYPE);
        if (preregisteredAutoType.containsKey(autoTypeMakerName))
            defaultSchemaMaker = preregisteredAutoType.get(autoTypeMakerName);
        else defaultSchemaMaker = ConfigurationUtil.instantiate(autoTypeMakerName);
        //Disable auto-type making when batch-loading is enabled since that may overwrite types without warning
        if (batchLoading) defaultSchemaMaker = DisableDefaultSchemaMaker.INSTANCE;

        hasDisabledSchemaConstraints = !configuration.get(SCHEMA_CONSTRAINTS);

        txVertexCacheSize = configuration.get(TX_CACHE_SIZE);
        //Check for explicit dirty vertex cache size first, then fall back on batch-loading-dependent default
        if (configuration.has(TX_DIRTY_SIZE)) {
            txDirtyVertexSize = configuration.get(TX_DIRTY_SIZE);
        } else {
            txDirtyVertexSize = batchLoading ?
                    TX_DIRTY_SIZE_DEFAULT_WITH_BATCH :
                    TX_DIRTY_SIZE_DEFAULT_WITHOUT_BATCH;
        }

        propertyPrefetching = configuration.get(PROPERTY_PREFETCHING);
        useMultiQuery = configuration.get(USE_MULTIQUERY);
        adjustQueryLimit = configuration.get(ADJUST_LIMIT);
        allowVertexIdSetting = configuration.get(ALLOW_SETTING_VERTEX_ID);
        logTransactions = configuration.get(SYSTEM_LOG_TRANSACTIONS);

        unknownIndexKeyName = configuration.get(IGNORE_UNKNOWN_INDEX_FIELD) ? UNKNOWN_FIELD_NAME : null;

        configureMetrics();
    }

    private void configureMetrics() {
        Preconditions.checkNotNull(configuration);

        metricsPrefix = configuration.get(METRICS_PREFIX);

        if (!configuration.get(BASIC_METRICS)) {
            metricsPrefix = null;
        } else {
            Preconditions.checkNotNull(metricsPrefix);
        }

        configureMetricsConsoleReporter();
        configureMetricsCsvReporter();
        configureMetricsJmxReporter();
        configureMetricsSlf4jReporter();
        configureMetricsGangliaReporter();
        configureMetricsGraphiteReporter();
    }

    private void configureMetricsConsoleReporter() {
        if (configuration.has(METRICS_CONSOLE_INTERVAL)) {
            MetricManager.INSTANCE.addConsoleReporter(configuration.get(METRICS_CONSOLE_INTERVAL));
        }
    }

    private void configureMetricsCsvReporter() {
        if (configuration.has(METRICS_CSV_DIR)) {
            MetricManager.INSTANCE.addCsvReporter(configuration.get(METRICS_CSV_INTERVAL), configuration.get(METRICS_CSV_DIR));
        }
    }

    private void configureMetricsJmxReporter() {
        if (configuration.get(METRICS_JMX_ENABLED)) {
            MetricManager.INSTANCE.addJmxReporter(configuration.get(METRICS_JMX_DOMAIN), configuration.get(METRICS_JMX_AGENTID));
        }
    }

    private void configureMetricsSlf4jReporter() {
        if (configuration.has(METRICS_SLF4J_INTERVAL)) {
            // null loggerName is allowed -- that means Metrics will use its internal default
            MetricManager.INSTANCE.addSlf4jReporter(configuration.get(METRICS_SLF4J_INTERVAL),
                    configuration.has(METRICS_SLF4J_LOGGER) ? configuration.get(METRICS_SLF4J_LOGGER) : null);
        }
    }

    private void configureMetricsGangliaReporter() {
        if (configuration.has(GANGLIA_HOST_OR_GROUP)) {
            final String host = configuration.get(GANGLIA_HOST_OR_GROUP);
            final Duration intervalDuration = configuration.get(GANGLIA_INTERVAL);
            final Integer port = configuration.get(GANGLIA_PORT);

            final UDPAddressingMode addressingMode;
            final String addressingModeString = configuration.get(GANGLIA_ADDRESSING_MODE);
            if (addressingModeString.equalsIgnoreCase("multicast")) {
                addressingMode = UDPAddressingMode.MULTICAST;
            } else if (addressingModeString.equalsIgnoreCase("unicast")) {
                addressingMode = UDPAddressingMode.UNICAST;
            } else throw new AssertionError();

            final Boolean proto31 = configuration.get(GANGLIA_USE_PROTOCOL_31);

            final int ttl = configuration.get(GANGLIA_TTL);

            final UUID uuid = configuration.has(GANGLIA_UUID)? UUID.fromString(configuration.get(GANGLIA_UUID)):null;

            String spoof = null;
            if (configuration.has(GANGLIA_SPOOF)) spoof = configuration.get(GANGLIA_SPOOF);

            try {
                MetricManager.INSTANCE.addGangliaReporter(host, port, addressingMode, ttl, proto31, uuid, spoof, intervalDuration);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void configureMetricsGraphiteReporter() {
        if (configuration.has(GRAPHITE_HOST)) {
            MetricManager.INSTANCE.addGraphiteReporter(configuration.get(GRAPHITE_HOST),
                    configuration.get(GRAPHITE_PORT),
                    configuration.get(GRAPHITE_PREFIX),
                    configuration.get(GRAPHITE_INTERVAL));
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean hasFlushIDs() {
        return flushIDs;
    }

    public boolean hasForceIndexUsage() {
        return forceIndexUsage;
    }

    public int getTxVertexCacheSize() {
        return txVertexCacheSize;
    }

    public int getTxDirtyVertexSize() {
        return txDirtyVertexSize;
    }

    public boolean isBatchLoading() {
        return batchLoading;
    }

    public String getUniqueGraphId() {
        return uniqueGraphId;
    }

    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    public DefaultSchemaMaker getDefaultSchemaMaker() {
        return defaultSchemaMaker;
    }

    public boolean hasDisabledSchemaConstraints() {
        return hasDisabledSchemaConstraints;
    }

    public boolean allowVertexIdSetting() {
        return allowVertexIdSetting;
    }

    public Duration getMaxCommitTime() {
        return configuration.get(MAX_COMMIT_TIME);
    }

    public Duration getMaxWriteTime() {
        return configuration.get(STORAGE_WRITE_WAITTIME);
    }

    public boolean hasPropertyPrefetching() {
        if (propertyPrefetching == null) {
            return getStoreFeatures().isDistributed();
        } else {
            return propertyPrefetching;
        }
    }

    public boolean useMultiQuery() {
        return useMultiQuery;
    }

    public boolean adjustQueryLimit() {
        return adjustQueryLimit;
    }

    public String getUnknownIndexKeyName() {
        return unknownIndexKeyName;
    }

    public boolean hasLogTransactions() {
        return logTransactions;
    }

    public TimestampProvider getTimestampProvider() {
        return configuration.get(TIMESTAMP_PROVIDER);
    }

    public static List<RegisteredAttributeClass<?>> getRegisteredAttributeClasses(Configuration configuration) {
        List<RegisteredAttributeClass<?>> all = new ArrayList<>();
        for (String attributeId : configuration.getContainedNamespaces(CUSTOM_ATTRIBUTE_NS)) {
            Preconditions.checkArgument(attributeId.startsWith(ATTRIBUTE_PREFIX),"Invalid attribute definition: %s",attributeId);
            int position;
            try {
                position = Integer.parseInt(attributeId.substring(ATTRIBUTE_PREFIX.length()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Expected entry of the form ["+ ATTRIBUTE_PREFIX +"X] where X is a number but given" + attributeId);
            }
            final Class<?> clazz;
            final AttributeSerializer<?> serializer;
            String classname = configuration.get(CUSTOM_ATTRIBUTE_CLASS,attributeId);
            try {
                clazz = Class.forName(classname);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not find attribute class" + classname, e);
            }
            Preconditions.checkNotNull(clazz);

            Preconditions.checkArgument(configuration.has(CUSTOM_SERIALIZER_CLASS, attributeId));
            String serializerName = configuration.get(CUSTOM_SERIALIZER_CLASS, attributeId);
            try {
                Class<?> serializerClass = Class.forName(serializerName);
                serializer = (AttributeSerializer) serializerClass.newInstance();
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not find serializer class" + serializerName);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException("Could not instantiate serializer class" + serializerName, e);
            }
            Preconditions.checkNotNull(serializer);
            RegisteredAttributeClass reg = new RegisteredAttributeClass(position, clazz, serializer);
            for (RegisteredAttributeClass<?> registeredAttributeClass : all) {
                if (registeredAttributeClass.equals(reg)) {
                    throw new IllegalArgumentException("Duplicate attribute registration: " + registeredAttributeClass
                            + " and " + reg);
                }
            }
            all.add(reg);

        }
        return all;
    }

    public VertexIDAssigner getIDAssigner(Backend backend) {
        return new VertexIDAssigner(configuration, backend.getIDAuthority(), backend.getStoreFeatures());
    }

    public String getBackendDescription() {
        String className = configuration.get(STORAGE_BACKEND);
        if (className.equalsIgnoreCase("berkeleyje")) {
            return className + ":" + configuration.get(STORAGE_DIRECTORY);
        } else {
            return className + ":" + Arrays.toString(configuration.get(STORAGE_HOSTS));
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Backend getBackend() {
        Backend backend = new Backend(configuration);
        backend.initialize(configuration);
        storeFeatures = backend.getStoreFeatures();
        return backend;
    }

    public String getGraphName() {
        return getConfigurationAtOpen().getString(GRAPH_NAME.toStringWithoutRoot());
    }

    public StoreFeatures getStoreFeatures() {
        Preconditions.checkArgument(storeFeatures != null, "Cannot retrieve store features before the storage backend has been initialized");
        return storeFeatures;
    }

    public Serializer getSerializer() {
        return getSerializer(configuration);
    }


    public static Serializer getSerializer(Configuration configuration) {
        Serializer serializer = new StandardSerializer();
        for (RegisteredAttributeClass<?> clazz : getRegisteredAttributeClasses(configuration)) {
            clazz.registerWith(serializer);
        }
        return serializer;
    }

    public SchemaCache getTypeCache(SchemaCache.StoreRetrieval retriever) {
        if (configuration.get(BASIC_METRICS)) return new MetricInstrumentedSchemaCache(retriever);
        else return new StandardSchemaCache(retriever);
    }

    public org.apache.commons.configuration.Configuration getLocalConfiguration() {
        org.apache.commons.configuration.Configuration config = ((CommonsConfiguration)localConfiguration.getConfiguration()).getCommonConfiguration();
        config.setProperty(Graph.GRAPH, JanusGraphFactory.class.getName());
        return config;
    }

    public org.apache.commons.configuration.Configuration getConfigurationAtOpen() {
        org.apache.commons.configuration.Configuration result = new BaseConfiguration();
        for (String k : configurationAtOpen.getKeys("")) {
            result.setProperty(k, configurationAtOpen.get(k, Object.class));
        }
        return result;
    }


	/* ----------------------------------------
     Methods for writing/reading config files
	-------------------------------------------*/

    public static String getPath(File dir) {
        return dir.getAbsolutePath() + File.separator;
    }

}
