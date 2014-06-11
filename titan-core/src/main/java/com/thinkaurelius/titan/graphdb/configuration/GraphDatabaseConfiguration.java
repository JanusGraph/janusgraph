package com.thinkaurelius.titan.graphdb.configuration;

import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.AttributeHandler;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.schema.DefaultSchemaMaker;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultSchemaMaker;
import com.thinkaurelius.titan.graphdb.types.typemaker.DisableDefaultSchemaMaker;
import com.thinkaurelius.titan.util.stats.NumberUtil;
import com.thinkaurelius.titan.diskstorage.util.time.*;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.KCVSConfiguration;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConflictAvoidanceMode;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLog;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;
import com.thinkaurelius.titan.graphdb.database.cache.MetricInstrumentedSchemaCache;
import com.thinkaurelius.titan.graphdb.database.cache.StandardSchemaCache;
import com.thinkaurelius.titan.graphdb.database.cache.SchemaCache;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import com.thinkaurelius.titan.util.encoding.LongEncoding;
import com.thinkaurelius.titan.util.system.NetworkUtil;

import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.management.MBeanServerFactory;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.util.stats.MetricManager;

/**
 * Provides functionality to configure a {@link com.thinkaurelius.titan.core.TitanGraph} INSTANCE.
 * <p/>
 * <p/>
 * A graph database configuration is uniquely associated with a graph database and must not be used for multiple
 * databases.
 * <p/>
 * After a graph database has been initialized with respect to a configuration, some parameters of graph database
 * configuration may no longer be modifiable.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class GraphDatabaseConfiguration {

    private static final Logger log =
            LoggerFactory.getLogger(GraphDatabaseConfiguration.class);


    public static ConfigNamespace ROOT_NS = new ConfigNamespace(null,"root","Root Configuration Namespace for the Titan Graph Database");

    // ################ GENERAL #######################
    // ################################################

    /**
     * Configures the {@link com.thinkaurelius.titan.core.schema.DefaultSchemaMaker} to be used by this graph. If set to 'none', automatic creation of types
     * is disabled.
     */
    public static final ConfigOption<String> AUTO_TYPE = new ConfigOption<String>(ROOT_NS,"autotype",
            "Configures the DefaultTypeMaker to be used by this graph. If set to 'none', automatic creation of types is disabled.",
            ConfigOption.Type.MASKABLE, "blueprints" , new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
            return s!=null && preregisteredAutoType.containsKey(s);
        }
    });
//    public static final String AUTO_TYPE_KEY = "autotype";
//    public static final String AUTO_TYPE_DEFAULT = "blueprints";

    private static final Map<String, DefaultSchemaMaker> preregisteredAutoType = new HashMap<String, DefaultSchemaMaker>() {{
        put("none", DisableDefaultSchemaMaker.INSTANCE);
        put("blueprints", BlueprintsDefaultSchemaMaker.INSTANCE);
    }};

    /**
     * If this option is enabled, a transaction will retrieval all of a vertex's properties when asking for any property.
     * This will significantly speed up subsequent property lookups on the same vertex, hence this option is enabled by default.
     * Disable this option when the graph contains vertices with very many properties such that retrieving all of them substantially
     * increases latencies compared to a single property retrieval.
     */
    public static final ConfigOption<Boolean> PROPERTY_PREFETCHING = new ConfigOption<Boolean>(ROOT_NS,"fast-property",
            "Whether to pre-fetch all properties on first vertex property access. This can eliminate backend calls on subsequent" +
                    "property access for the same vertex at the expense of retrieving all properties at once.",
            ConfigOption.Type.MASKABLE, Boolean.class);
//    public static final String PROPERTY_PREFETCHING_KEY = "fast-property";

    /**
     * When enabled, Titan will accept user provided vertex ids as long as they are valid Titan vertex ids - see
     * {@link com.thinkaurelius.titan.core.util.TitanId#toVertexId(long)}. When enabled, Titan will now longer allocate and assign
     * ids internally, so all vertices must be added through {@link com.thinkaurelius.titan.core.TitanTransaction#addVertex(Long)}.
     * <p/>
     * Use this setting WITH GREAT CARE since it can easily lead to data corruption and performance issues when not used correctly.
     * This should only ever be used when mapping external to internal ids causes performance issues at very large scale.
     */
    public static final ConfigOption<Boolean> ALLOW_SETTING_VERTEX_ID = new ConfigOption<Boolean>(ROOT_NS,"set-vertex-id",
            "Whether user provided vertex ids should be enabled and Titan's automatic id allocation be disabled. " +
                "Useful when operating Titan in concert with another storage system that assigns long ids but disables some" +
                    "of Titan's advanced features. EXPERT FEATURE - USE WITH GREAT CARE.",
            ConfigOption.Type.FIXED, false);

    public static final ConfigOption<Boolean> FORCE_INDEX_USAGE = new ConfigOption<Boolean>(ROOT_NS,"force-index",
            "Whether Titan should throw an exception if a graph query cannot be answered using an index. Doing so" +
                    "limits the functionality of Titan's graph queries but ensures that slow graph queries are avoided.",
            ConfigOption.Type.MASKABLE, false);
//
//    public static final String ALLOW_SETTING_VERTEX_ID_KEY = "set-vertex-id";
//    public static final boolean ALLOW_SETTING_VERTEX_ID_DEFAULT = false;


    /**
     * When true, Titan will ignore unknown index key fields in queries.
     */
    public static final ConfigOption<Boolean> IGNORE_UNKNOWN_INDEX_FIELD = new ConfigOption<Boolean>(ROOT_NS, "ignore-unknown-index-key",
            "Whether to ignore undefined types encountered in user-provided index queries",
            ConfigOption.Type.MASKABLE, false);
//    public static final String IGNORE_UNKNOWN_INDEX_FIELD_KEY = "ignore-unknown-index-key";
//    public static final boolean IGNORE_UNKNOWN_INDEX_FIELD_DEFAULT = false;

    public static final String UKNOWN_FIELD_NAME = "unknown_key";


    public static final ConfigOption<Timestamps> TIMESTAMP_PROVIDER = new ConfigOption<Timestamps>(ROOT_NS, "timestamps",
            "The timestamp resolution to use when writing to storage and indices",
            ConfigOption.Type.FIXED, Timestamps.MICRO);


    public static final ConfigOption<Boolean> SYSTEM_LOG_TRANSACTIONS = new ConfigOption<Boolean>(ROOT_NS,"log-tx",
            "Whether transaction mutations should be logged to Titan's system log",
            ConfigOption.Type.GLOBAL, true);

    public static final ConfigOption<String> UNIQUE_INSTANCE_ID = new ConfigOption<String>(ROOT_NS,"unique-instance-id",
            "Unique identifier for this Titan instance.  This must be unique among all instances " +
            "concurrently accessing the same stores or indexes.  It's automatically generated by " +
            "concatenating the hostname, process id, and a static (process-wide) counter. " +
            "Leaving it unset is recommended.",
            ConfigOption.Type.LOCAL, String.class);


    public static final ConfigOption<Short> UNIQUE_INSTANCE_ID_SUFFIX = new ConfigOption<Short>(ROOT_NS,"unique-instance-id-suffix",
            "When this is set and " + UNIQUE_INSTANCE_ID.getName() + " is not, this Titan " +
            "instance's unique identifier is generated by concatenating the hostname to the " +
            "provided number.  This is a legacy option which is currently only useful if the JVM's " +
            "ManagementFactory.getRuntimeMXBean().getName() is not unique between processes.",
            ConfigOption.Type.LOCAL, Short.class);

    public static final ConfigOption<String> INITIAL_TITAN_VERSION = new ConfigOption<String>(ROOT_NS,"titan-version",
            "The version of Titan with which this database was created.  Don't manually set this property.",
            ConfigOption.Type.FIXED, String.class);

    // ################ INSTANCE REGISTRATION #######################
    // ##############################################################

    public static final ConfigNamespace REGISTRATION_NS = new ConfigNamespace(ROOT_NS,"system-registration",
            "This is used internally to keep track of open instances.",true);

    public static final ConfigOption<Timepoint> REGISTRATION_TIME = new ConfigOption<Timepoint>(REGISTRATION_NS,"startup-time",
            "Timestamp when this instance was started.  Automatically set.", ConfigOption.Type.GLOBAL, Timepoint.class);

    // ################ CACHE #######################
    // ################################################

//    public static final String CACHE_NAMESPACE = "cache";
    public static final ConfigNamespace CACHE_NS = new ConfigNamespace(ROOT_NS,"cache","Configuration options that modify Titan's caching behavior");

    /**
     * Whether this Titan instance should use a database level cache in front of the
     * storage backend in order to speed up frequent queries across transactions
     */
//    public static final String DB_CACHE_KEY = "db-cache";
//    public static final boolean DB_CACHE_DEFAULT = false;
    public static final ConfigOption<Boolean> DB_CACHE = new ConfigOption<Boolean>(CACHE_NS,"db-cache",
            "Whether to enable Titan's database-level cache, which is shared across all transactions. " +
            "Enabling this option speeds up traversals by holding hot graph elements in memory, " +
            "but also increases the likelihood of reading stale data.  Disabling it forces each " +
            "transaction to indepedendently fetch graph elements from storage before reading/writing them.",
            ConfigOption.Type.MASKABLE, false);

    /**
     * The size of the database level cache.
     * If this value is between 0.0 (strictly bigger) and 1.0 (strictly smaller), then it is interpreted as a
     * percentage of the total heap space available to the JVM this Titan instance is running in.
     * If this value is bigger than 1.0 it is interpreted as an absolute size in bytes.
     */
//    public static final String DB_CACHE_SIZE_KEY = "db-cache-size";
//    public static final double DB_CACHE_SIZE_DEFAULT = 0.3;
    public static final ConfigOption<Double> DB_CACHE_SIZE = new ConfigOption<Double>(CACHE_NS,"db-cache-size",
            "Size of Titan's database level cache.  Values between 0 and 1 are interpreted as a percentage " +
            "of VM heap, while larger values are interpreted as an absolute size in bytes.",
            ConfigOption.Type.MASKABLE, 0.3);

    /**
     * How long the database level cache will keep keys expired while the mutations that triggered the expiration
     * are being persisted. This value should be larger than the time it takes for persisted mutations to become visible.
     * This setting only ever makes sense for distributed storage backends where writes may be accepted but are not
     * immediately readable.
     */
//    public static final String DB_CACHE_CLEAN_WAIT_KEY = "db-cache-clean-wait";
//    public static final long DB_CACHE_CLEAN_WAIT_DEFAULT = 50;
    public static final ConfigOption<Integer> DB_CACHE_CLEAN_WAIT = new ConfigOption<Integer>(CACHE_NS,"db-cache-clean-wait",
            "How long, in milliseconds, database-level cache will keep entries after flushing them." +
            "This option is only useful on distributed storage backends that are capable of acknowledging writes " +
            "without necessarily making them immediately visible.",
            ConfigOption.Type.GLOBAL_OFFLINE, 50);

    /**
     * The default expiration time for elements held in the database level cache. This is the time period before
     * Titan will check against storage backend for a newer query answer.
     * Setting this value to 0 will cache elements forever (unless they get evicted due to space constraints). This only
     * makes sense when this is the only Titan instance interacting with a storage backend.
     */
//    public static final String DB_CACHE_TIME_KEY = "db-cache-time";
//    public static final long DB_CACHE_TIME_DEFAULT = 10000;
    public static final ConfigOption<Long> DB_CACHE_TIME = new ConfigOption<Long>(CACHE_NS,"db-cache-time",
            "Default expiration time, in milliseconds, for entries in the database-level cache. " +
            "Entries are evicted when they reach this age even if the cache has room to spare. " +
            "Set to 0 to disable expiration (cache entries live forever).",
            ConfigOption.Type.GLOBAL_OFFLINE, 10000l);

    /**
     * Configures the maximum number of recently-used vertices cached by a transaction. The smaller the cache size, the
     * less memory a transaction can consume at maximum. For many concurrent, long running transactions in memory constraint
     * environments, reducing the cache size can avoid OutOfMemory and GC limit exceeded exceptions.
     * Note, however, that all modifications in a transaction must always be kept in memory and hence this setting does not
     * have much impact on write intense transactions. Those must be split into smaller transactions in the case of memory errors.
     * <p/>
     * The recently-used vertex cache can contain both dirty and clean vertices, that is, both vertices which have been
     * created or updated in the current transaction and vertices which have only been read in the current transaction.
     */
//    public static final String TX_CACHE_SIZE_KEY = "tx-cache-size";
//    public static final int TX_CACHE_SIZE_DEFAULT = 20000;
    public static final ConfigOption<Integer> TX_CACHE_SIZE = new ConfigOption<Integer>(CACHE_NS,"tx-cache-size",
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
    public static final ConfigOption<Integer> TX_DIRTY_SIZE = new ConfigOption<Integer>(CACHE_NS, "tx-dirty-size",
          "Initial size of the transaction-level cache of uncommitted dirty vertices. " +
          "This is a performance hint for write-heavy, performance-sensitive transactional workloads. " +
          "If set, it should roughly match the median vertices modified per transaction.",
          ConfigOption.Type.MASKABLE, Integer.class);

    /**
     * The default value of {@link #TX_DIRTY_SIZE} when batch loading is disabled.
     * This value is only considered if the user does not specify a value for
     * {@code #TX_DIRTY_CACHE_SIZE} explictly in either the graph or transaction config.
     */
    private static final int TX_DIRTY_SIZE_DEFAULT_WITHOUT_BATCH = 32;

    /**
     * The default value of {@link #TX_DIRTY_SIZE} when batch loading is enabled.
     * This value is only considered if the user does not specify a value for
     * {@code #TX_DIRTY_CACHE_SIZE} explictly in either the graph or transaction config.
     */
    private static final int TX_DIRTY_SIZE_DEFAULT_WITH_BATCH = 4096;


    // ################ STORAGE #######################
    // ################################################

//    public static final String STORAGE_NAMESPACE = "storage";
    public static final ConfigNamespace STORAGE_NS = new ConfigNamespace(ROOT_NS,"storage","Configuration options for the storage backend.  Some options are applicable only for certain backends.");
    public static final ConfigNamespace STORAGE_SSL_NS = new ConfigNamespace(STORAGE_NS, "ssl", "Configuration options for SSL");
    public static final ConfigNamespace STORAGE_SSL_TRUSTSTORE = new ConfigNamespace(STORAGE_SSL_NS, "truststore", "Configuration options for SSL Truststore.");


    /**
     * Storage directory for those storage backends that require local storage
     */
    public static final ConfigOption<String> STORAGE_DIRECTORY = new ConfigOption<String>(STORAGE_NS,"directory",
            "Storage directory for those storage backends that require local storage",
            ConfigOption.Type.LOCAL, String.class);
//    public static final String STORAGE_DIRECTORY_KEY = "directory";

    /**
     * Path to a configuration file for those storage backends that
     * require/support a separate config file
     */
    public static final ConfigOption<String> STORAGE_CONF_FILE = new ConfigOption<String>(STORAGE_NS,"conf-file",
            "Path to a configuration file for those storage backends that require/support a separate config file",
            ConfigOption.Type.LOCAL, String.class);
//    public static final String STORAGE_CONF_FILE_KEY = "conffile";

    /**
     * Define the storage backed to use for persistence
     */
    public static final ConfigOption<String> STORAGE_BACKEND = new ConfigOption<String>(STORAGE_NS,"backend",
            "Either the package and classname of a StoreManager implementation or one of " +
            "Titan's built-in shorthand names for its standard storage backends.",
            ConfigOption.Type.LOCAL, "local");
//    public static final String STORAGE_BACKEND_KEY = "backend";
//    public static final String STORAGE_BACKEND_DEFAULT = "local";

    /**
     * Specifies whether this database is read-only, i.e. write operations are not supported
     */
    public static final ConfigOption<Boolean> STORAGE_READONLY = new ConfigOption<Boolean>(STORAGE_NS,"read-only",
            "Read-only database",
            ConfigOption.Type.LOCAL, false);
//    public static final String STORAGE_READONLY_KEY = "read-only";
//    public static final boolean STORAGE_READONLY_DEFAULT = false;

    /**
     * Enables batch loading which improves write performance but assumes that only one thread is interacting with
     * the graph
     */
    public static final ConfigOption<Boolean> STORAGE_BATCH = new ConfigOption<Boolean>(STORAGE_NS,"batch-loading",
            "Whether to enable batch loading into the storage backend",
            ConfigOption.Type.LOCAL, false);
//    public static final String STORAGE_BATCH_KEY = "batch-loading";
//    public static final boolean STORAGE_BATCH_DEFAULT = false;

    /**
     * Enables transactions on storage backends that support them
     */
    public static final ConfigOption<Boolean> STORAGE_TRANSACTIONAL = new ConfigOption<Boolean>(STORAGE_NS,"transactions",
            "Enables transactions on storage backends that support them",
            ConfigOption.Type.MASKABLE, true);
//    public static final String STORAGE_TRANSACTIONAL_KEY = "transactions";
//    public static final boolean STORAGE_TRANSACTIONAL_DEFAULT = true;

    /**
     * Buffers graph mutations locally up to the specified number before persisting them against the storage backend.
     * Set to 0 to disable buffering. Buffering is disabled automatically if the storage backend does not support buffered mutations.
     */
    public static final ConfigOption<Integer> BUFFER_SIZE = new ConfigOption<Integer>(STORAGE_NS,"buffer-size",
            "Size of the batch in which mutations are persisted",
            ConfigOption.Type.MASKABLE, 1024, ConfigOption.positiveInt());
//    public static final String BUFFER_SIZE_KEY = "buffer-size";
//    public static final int BUFFER_SIZE_DEFAULT = 1024;

    /**
     * Number of times the database attempts to persist the transactional state to the storage layer.
     * Persisting the state of a committed transaction might fail for various reasons, some of which are
     * temporary such as network failures. For temporary failures, Titan will re-attempt to persist the
     * state up to the number of times specified.
     */
//    public static final ConfigOption<Integer> WRITE_ATTEMPTS = new ConfigOption<Integer>(STORAGE_NS,"write-attempts",
//            "Number of attempts for write operations that might experience temporary failures",
//            ConfigOption.Type.MASKABLE, 5, ConfigOption.positiveInt());
//    public static final String WRITE_ATTEMPTS_KEY = "write-attempts";
//    public static final int WRITE_ATTEMPTS_DEFAULT = 5;

    /**
     * Number of times the database attempts to execute a read operation against the storage layer in the current transaction.
     * A read operation might fail for various reasons, some of which are
     * temporary such as network failures. For temporary failures, Titan will re-attempt to read the
     * state up to the number of times specified before failing the transaction
     */
//    public static final ConfigOption<Integer> READ_ATTEMPTS = new ConfigOption<Integer>(STORAGE_NS,"read-attempts",
//            "Number of attempts for read operations that might experience temporary failures",
//            ConfigOption.Type.MASKABLE, 3, ConfigOption.positiveInt());
//    public static final String READ_ATTEMPTS_KEY = "read-attempts";
//    public static final int READ_ATTEMPTS_DEFAULT = 3;

    public static final ConfigOption<Duration> STORAGE_WRITE_WAITTIME = new ConfigOption<Duration>(STORAGE_NS,"write-time",
            "Maximum time (in ms) to wait for a backend write operation to complete successfully. If a backend write operation" +
            "fails temporarily, Titan will backoff exponentially and retry the operation until the wait time has been exhausted. ",
            ConfigOption.Type.MASKABLE, new StandardDuration(100000L, TimeUnit.MILLISECONDS));

    public static final ConfigOption<Duration> STORAGE_READ_WAITTIME = new ConfigOption<Duration>(STORAGE_NS,"read-time",
            "Maximum time (in ms) to wait for a backend read operation to complete successfully. If a backend read operation" +
                    "fails temporarily, Titan will backoff exponentially and retry the operation until the wait time has been exhausted. ",
            ConfigOption.Type.MASKABLE, new StandardDuration(10000L, TimeUnit.MILLISECONDS));


    /**
     * If enabled, Titan attempts to parallelize storage operations against the storage backend using a fixed thread pool shared
     * across the entire Titan graph database instance. Parallelization is only applicable to certain storage operations and
     * can be beneficial when the operation is I/O bound.
     */
    public static final ConfigOption<Boolean> PARALLEL_BACKEND_OPS = new ConfigOption<Boolean>(STORAGE_NS,"parallel-backend-ops",
            "Whether Titan should attempt to parallelize storage operations",
            ConfigOption.Type.MASKABLE, true);
//    public static final String PARALLEL_BACKEND_OPS_KEY = "parallel-backend-ops";
//    public static final boolean PARALLEL_BACKEND_OPS_DEFAULT = true;

    /**
     * A unique identifier for the machine running the TitanGraph instance.
     * It must be ensured that no other machine accessing the storage backend can have the same identifier.
     */
//    public static final ConfigOption<String> INSTANCE_RID_RAW = new ConfigOption<String>(STORAGE_NS,"machine-id",
//            "A unique identifier for the machine running the TitanGraph instance",
//            ConfigOption.Type.LOCAL, String.class);
//    public static final String INSTANCE_RID_RAW_KEY = "machine-id";

    public static final ConfigOption<String[]> STORAGE_HOSTS = new ConfigOption<String[]>(STORAGE_NS,"hostname",
            "Configuration key for the hostname or list of hostname of remote storage backend servers to connect to",
            ConfigOption.Type.LOCAL, new String[]{NetworkUtil.getLoopbackAddress()});
    /**
     * Configuration key for the hostname or list of hostname of remote storage backend servers to connect to.
     * <p/>
     * Value = {@value}
     */
//    public static final String HOSTNAME_KEY = "hostname";
    /**
     * Default hostname at which to attempt connecting to remote storage backend
     * <p/>
     * Value = {@value}
     */
//    public static final String HOSTNAME_DEFAULT = NetworkUtil.getLoopbackAddress();

    /**
     * Configuration key for the port on which to connect to remote storage backend servers.
     * <p/>
     * Value = {@value}
     */
    public static final ConfigOption<Integer> STORAGE_PORT = new ConfigOption<Integer>(STORAGE_NS,"port",
            "Configuration key for the port on which to connect to remote storage backend servers",
            ConfigOption.Type.LOCAL, Integer.class);

//    public static final String PORT_KEY = "port";

    /**
     * Whether the storage backend should compress the data when storing it on disk.
     * Compression reduces the size on disk and speed up I/0 operations if the data is efficiently compressable.
     */
    public static final ConfigOption<Boolean> STORAGE_COMPRESSION = new ConfigOption<Boolean>(STORAGE_NS,"compression",
            "Whether the storage backend should use compression when storing the data",
            ConfigOption.Type.FIXED, true);

    /**
     * The size of blocks that are compressed individually in kilobyte
     */
    public static final ConfigOption<Integer> STORAGE_COMPRESSION_SIZE = new ConfigOption<Integer>(STORAGE_NS,"compression-block-size",
            "The size of the compression blocks in kilobytes",
            ConfigOption.Type.FIXED, 64);



    public static final ConfigOption<Integer> REPLICATION_FACTOR = new ConfigOption<Integer>(STORAGE_NS,"replication-factor",
            "The number of data replicas (including the original copy) that should be kept. " +
            "This is only meaningful for storage backends that natively support data replication.",
            ConfigOption.Type.GLOBAL_OFFLINE, 1);

    /**
     * Username and password keys to be used to specify an access credential that may be needed to connect
     * with a secured storage backend.
     */
    public static final ConfigOption<String> AUTH_USERNAME = new ConfigOption<String>(STORAGE_NS,"username",
            "Username to authenticate against backend",
            ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> AUTH_PASSWORD = new ConfigOption<String>(STORAGE_NS,"password",
            "Password to authenticate against backend",
            ConfigOption.Type.LOCAL, String.class);
//    public static final String AUTH_USERNAME_KEY = "username";
//    public static final String AUTH_PASSWORD_KEY = "password";

    /**
     * Default timeout when connecting to a remote database instance
     * <p/>
     */
    public static final ConfigOption<Duration> CONNECTION_TIMEOUT = new ConfigOption<Duration>(STORAGE_NS,"connection-timeout",
            "Default timeout, in milliseconds, when connecting to a remote database instance",
            ConfigOption.Type.MASKABLE, Duration.class, new StandardDuration(10000L, TimeUnit.MILLISECONDS));
//    public static final int CONNECTION_TIMEOUT_DEFAULT = 10000;
//    public static final String CONNECTION_TIMEOUT_KEY = "connection-timeout";

    /**
     * Time in milliseconds for backend manager to wait for the storage backends to
     * become available when Titan is run in server mode. Should the backend manager
     * experience exceptions when attempting to access the storage backend it will retry
     * until this timeout is exceeded.
     * <p/>
     * A wait time of 0 disables waiting.
     * <p/>
     */
    public static final ConfigOption<Duration> SETUP_WAITTIME = new ConfigOption<Duration>(STORAGE_NS,"setup-wait",
            "Time in milliseconds for backend manager to wait for the storage backends to become available when Titan is run in server mode",
            ConfigOption.Type.MASKABLE, Duration.class, new StandardDuration(60000L, TimeUnit.MILLISECONDS));
//    public static final int SETUP_WAITTIME_DEFAULT = 60000;
//    public static final String SETUP_WAITTIME_KEY = "setup-wait";

    /**
     * Default number of connections to pool when connecting to a remote database.
     * <p/>
     * Value = {@value}
     */
    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE = new ConfigOption<Integer>(STORAGE_NS,"connection-pool-size",
            "Default number of connections to pool when connecting to a remote database",
            ConfigOption.Type.MASKABLE, 32);
//    public static final int CONNECTION_POOL_SIZE_DEFAULT = 32;
//    public static final String CONNECTION_POOL_SIZE_KEY = "connection-pool-size";

    /**
     * Default number of results to pull over the wire when iterating over a distributed
     * storage backend.
     * This is batch size of results to pull when iterating a result set.
     */
    public static final ConfigOption<Integer> PAGE_SIZE = new ConfigOption<Integer>(STORAGE_NS,"page-size",
            "Titan break requests that may return many results from distributed storage backends " +
            "into a series of requests for small chunks/pages of results, where each chunk contains " +
            "up to this many elements.",
            ConfigOption.Type.MASKABLE, 100);
//    public static final int PAGE_SIZE_DEFAULT = 100;
//    public static final String PAGE_SIZE_KEY = "page-size";



    /**
     * Number of times the system attempts to acquire a lock before giving up and throwing an exception.
     */
    public static final ConfigOption<Integer> LOCK_RETRY = new ConfigOption<Integer>(STORAGE_NS,"lock-retries",
            "Number of times the system attempts to acquire a lock before giving up and throwing an exception",
            ConfigOption.Type.MASKABLE, 3);
//    public static final String LOCK_RETRY_COUNT = "lock-retries";
//    public static final int LOCK_RETRY_COUNT_DEFAULT = 3;
    /**
     * The number of milliseconds the system waits for a lock application to be acknowledged by the storage backend.
     * Also, the time waited at the end of all lock applications before verifying that the applications were successful.
     * This value should be a small multiple of the average consistent write time.
     */
    public static final ConfigOption<Duration> LOCK_WAIT = new ConfigOption<Duration>(STORAGE_NS,"lock-wait-time",
            "Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.class, new StandardDuration(100L, TimeUnit.MILLISECONDS));
//    public static final String LOCK_WAIT_MS = "lock-wait-time";
//    public static final long LOCK_WAIT_MS_DEFAULT = 100;

    /**
     * Number of milliseconds after which a lock is considered to have expired. Lock applications that were not released
     * are considered expired after this time and released.
     * This value should be larger than the maximum time a transaction can take in order to guarantee that no correctly
     * held applications are expired pre-maturely and as small as possible to avoid dead lock.
     */
    public static final ConfigOption<Duration> LOCK_EXPIRE = new ConfigOption<Duration>(STORAGE_NS,"lock-expiry-time",
            "Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.class, new StandardDuration(300 * 1000L, TimeUnit.MILLISECONDS));
//    public static final String LOCK_EXPIRE_MS = "lock-expiry-time";
//    public static final long LOCK_EXPIRE_MS_DEFAULT = 300 * 1000;

    /**
     * Whether to attempt to delete expired locks from the storage backend. True
     * will attempt to delete expired locks in a background daemon thread. False
     * will never attempt to delete expired locks. This option is only
     * meaningful for the default lock backend.
     *
     * @see #LOCK_BACKEND
     */
    public static final ConfigOption<Boolean> LOCK_CLEAN_EXPIRED = new ConfigOption<Boolean>(STORAGE_NS, "lock-clean-expired",
            "Whether to delete expired locks from the storage backend",
            ConfigOption.Type.MASKABLE, false);

    /**
     * Locker type to use.  The supported types are in {@link com.thinkaurelius.titan.diskstorage.Backend}.
     */
    public static final ConfigOption<String> LOCK_BACKEND = new ConfigOption<String>(STORAGE_NS,"lock-backend",
            "Locker type to use",
            ConfigOption.Type.GLOBAL_OFFLINE, "consistentkey");
//    public static final String LOCK_BACKEND = "lock-backend";
//    public static final String LOCK_BACKEND_DEFAULT = "consistentkey";


    // ################ STORAGE - META #######################

    public static final ConfigNamespace STORE_META_NS = new ConfigNamespace(STORAGE_NS,"meta","Meta data to include in storage backend retrievals",true);

    public static final ConfigOption<Boolean> STORE_META_TIMESTAMPS = new ConfigOption<Boolean>(STORE_META_NS,"timestamps",
            "Whether to include timestamps in retrieved entries for storage backends that automatically annotated entries with timestamps",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Boolean> STORE_META_TTL = new ConfigOption<Boolean>(STORE_META_NS,"ttl",
            "Whether to include ttl in retrieved entries for storage backends that automatically annotated entries with timestamps",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Boolean> STORE_META_VISIBILITY = new ConfigOption<Boolean>(STORE_META_NS,"visibility",
            "Whether to include visibility in retrieved entries for storage backends that automatically annotated entries with timestamps",
            ConfigOption.Type.GLOBAL, true);

    // ################ CLUSTERING ###########################
    // ################################################

    public static final ConfigNamespace CLUSTER_NS = new ConfigNamespace(ROOT_NS,"cluster","Configuration options for multi-machine deployments");

    /**
     * Whether the id space should be partitioned for equal distribution of keys. If the keyspace is ordered, this needs to be
     * enabled to ensure an even distribution of data. If the keyspace is random/hashed, then enabling this only has the benefit
     * of de-congesting a single id pool in the database.
     */
    public static final ConfigOption<Boolean> CLUSTER_PARTITION = new ConfigOption<Boolean>(CLUSTER_NS,"partition",
            "Whether the graph's element IDs should be randomly distributed across the space of available IDs " +
            "(true) or allocated in increasing order (false). Unless explicitly set, this defaults false for  " +
            "stores that hash keys and defaults true for stores that preserve key order (such as HBase and Cassandra " +
            "with ByteOrderedPartitioner).",
            ConfigOption.Type.FIXED, false);
//    public static final String IDS_PARTITION_KEY = "partition";
//    public static final boolean IDS_PARTITION_DEFAULT = false;

    public static final ConfigOption<Integer> CLUSTER_MAX_PARTITIONS = new ConfigOption<Integer>(CLUSTER_NS,"max-partitions",
            "The maximum number of ID partitions in the graph. Must be bigger than 1 and a power of 2.",
            ConfigOption.Type.FIXED, 64, new Predicate<Integer>() {
        @Override
        public boolean apply(@Nullable Integer integer) {
            return integer!=null && integer>1 && NumberUtil.isPowerOf2(integer);
        }
    });



    // ################ IDS ###########################
    // ################################################

    public static final ConfigNamespace IDS_NS = new ConfigNamespace(ROOT_NS,"ids","General configuration options for graph element IDs");

//    public static final String IDS_NAMESPACE = "ids";

    /**
     * Size of the block to be acquired. Larger block sizes require fewer block applications but also leave a larger
     * fraction of the id pool occupied and potentially lost. For write heavy applications, larger block sizes should
     * be chosen.
     */
    public static final ConfigOption<Integer> IDS_BLOCK_SIZE = new ConfigOption<Integer>(IDS_NS,"block-size",
            "Globally reserve graph element IDs in chunks of this size.  Setting this too low will make commits " +
            "frequently block on slow reservation requests.  Setting it too high will result in IDs wasted when a " +
            "graph instance shuts down with reserved but mostly-unused blocks.",
            ConfigOption.Type.GLOBAL_OFFLINE, 10000);
//    public static final String IDS_BLOCK_SIZE_KEY = "block-size";
//    public static final int IDS_BLOCK_SIZE_DEFAULT = 10000;

    /**
     * If flush ids is enabled, vertices and edges are assigned ids immediately upon creation. If not, then ids are only
     * assigned when the transaction is committed.
     */
    public static final ConfigOption<Boolean> IDS_FLUSH = new ConfigOption<Boolean>(IDS_NS,"flush",
            "When true, vertices and edges are assigned IDs immediately upon creation.  When false, " +
            "IDs are assigned only when the transaction commits.",
            ConfigOption.Type.MASKABLE, true);
//    public static final String IDS_FLUSH_KEY = "flush";
//    public static final boolean IDS_FLUSH_DEFAULT = true;

    /**
     * The number of milliseconds that the Titan id pool manager will wait before giving up on allocating a new block
     * of ids. Note, that failure to allocate a new id block will cause the entire database to fail, hence this value
     * should be set conservatively. Choose a high value if there is a lot of contention around id allocation.
     */
    public static final ConfigOption<Duration> IDS_RENEW_TIMEOUT = new ConfigOption<Duration>(IDS_NS,"renew-timeout",
            "The number of milliseconds that the Titan id pool manager will wait before giving up on allocating a new block of ids",
            ConfigOption.Type.MASKABLE, Duration.class, new StandardDuration(120000L, TimeUnit.MILLISECONDS));
//    public static final String IDS_RENEW_TIMEOUT_KEY = "renew-timeout";
//    public static final long IDS_RENEW_TIMEOUT_DEFAULT = 60 * 1000; // 1 minute

    /**
     * Configures when the id pool manager will attempt to allocate a new id block. When all but the configured percentage
     * of the current block is consumed, a new block will be allocated. Larger values should be used if a lot of ids
     * are allocated in a short amount of time. Value must be in (0,1].
     */
    public static final ConfigOption<Double> IDS_RENEW_BUFFER_PERCENTAGE = new ConfigOption<Double>(IDS_NS,"renew-percentage",
            "When the most-recently-reserved ID block has only this percentage of its total IDs remaining " +
            "(expressed as a value between 0 and 1), Titan asynchronously begins reserving another block. " +
            "This helps avoid transaction commits waiting on ID reservation even if the block size is relatively small.",
            ConfigOption.Type.MASKABLE, 0.3);
//    public static final String IDS_RENEW_BUFFER_PERCENTAGE_KEY = "renew-percentage";
//    public static final double IDS_RENEW_BUFFER_PERCENTAGE_DEFAULT = 0.3; // 30 %

    // ################ IDAUTHORITY ###################
    // ################################################

    //    public static final String STORAGE_NAMESPACE = "storage";
    public static final ConfigNamespace IDAUTHORITY_NS = new ConfigNamespace(IDS_NS,"authority","Configuration options for graph element ID reservation/allocation");

    /**
     * The number of milliseconds the system waits for an id block application to be acknowledged by the storage backend.
     * Also, the time waited after the application before verifying that the application was successful.
     */
    public static final ConfigOption<Duration> IDAUTHORITY_WAIT = new ConfigOption<Duration>(IDAUTHORITY_NS,"wait-time",
            "The number of milliseconds the system waits for an ID block reservation to be acknowledged by the storage backend",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.class, new StandardDuration(300L, TimeUnit.MILLISECONDS));
//    public static final String IDAUTHORITY_WAIT_MS_KEY = "idauthority-wait-time";
//    public static final long IDAUTHORITY_WAIT_MS_DEFAULT = 300;

    /**
     * Sets the strategy used by {@link ConsistentKeyIDAuthority} to avoid
     * contention in ID block alloction between Titan instances concurrently
     * sharing a single distributed storage backend.
     */
    // This is set to GLOBAL_OFFLINE as opposed to MASKABLE or GLOBAL to prevent mixing both global-randomized and local-manual modes within the same cluster
    public static final ConfigOption<ConflictAvoidanceMode> IDAUTHORITY_CONFLICT_AVOIDANCE = new ConfigOption<ConflictAvoidanceMode>(IDAUTHORITY_NS,"conflict-avoidance-mode",
            "This setting helps separate Titan instances sharing a single graph storage backend avoid contention when reserving ID blocks, " +
            "increasing overall throughput.",
            ConfigOption.Type.GLOBAL_OFFLINE, ConflictAvoidanceMode.NONE);

    /**
     * When Titan allocates IDs with {@link #IDAUTHORITY_RANDOMIZE_UNIQUEID}
     * enabled, it picks a random unique ID marker and attempts to allocate IDs
     * from a partition using the marker. The ID markers function as
     * subpartitions with each ID partition. If the attempt fails because that
     * partition + uniqueid combination is already completely allocated, then
     * Titan will generate a new random unique ID and try again. This controls
     * the maximum number of attempts before Titan assumes the entire partition
     * is allocated and fails the request. It must be set to at least 1 and
     * should generally be set to 3 or more.
     * <p/>
     * This setting has no effect when {@link #IDAUTHORITY_RANDOMIZE_UNIQUEID}
     * is disabled.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_RETRIES = new ConfigOption<Integer>(IDAUTHORITY_NS,"randomized-conflict-avoidance-retries",
            "Number of times the system attempts attempts ID block reservations with random conflict avoidance tags before giving up and throwing an exception",
            ConfigOption.Type.MASKABLE, 5);
//    public static final String IDAUTHORITY_RETRY_COUNT_KEY = "idauthority-retries";
//    public static final int IDAUTHORITY_RETRY_COUNT_DEFAULT = 20;

    /**
     * Configures the number of bits of Titan assigned ids that are reserved for a unique id marker that
     * allows the id allocation to be scaled over multiple sub-clusters and to reduce race-conditions
     * when a lot of Titan instances attempt to allocate ids at the same time (e.g. during parallel bulk loading)
     *
     * IMPORTANT: This should never ever, ever be modified from its initial value and ALL Titan instances must use the
     * same value. Otherwise, data corruption will occur.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_BITS = new ConfigOption<Integer>(IDAUTHORITY_NS,"conflict-avoidance-tag-bits",
            "Configures the number of bits of Titan-assigned element IDs that are reserved for the conflict avoidance tag",
            ConfigOption.Type.FIXED, 5 , new Predicate<Integer>() {
        @Override
        public boolean apply(@Nullable Integer uniqueIdBitWidth) {
            return uniqueIdBitWidth>=0 && uniqueIdBitWidth<=16;
        }
    });
//    public static final String IDAUTHORITY_UNIQUE_ID_BITS_KEY = "idauthority-uniqueid-bits";
//    public static final int IDAUTHORITY_UNIQUE_ID_BITS_DEFAULT = 0;

    /**
     * Unique id marker to be used by this Titan instance when allocating ids. The unique id marker
     * must be non-negative and fit within the number of unique id bits configured.
     * By assigning different unique id markers to individual Titan instances it can be assured
     * that those instances don't conflict with one another when attempting to allocate new id blocks.
     *
     * IMPORTANT: The configured unique id marker must fit within the configured unique id bit width.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_TAG = new ConfigOption<Integer>(IDAUTHORITY_NS,"conflict-avoidance-tag",
            "Conflict avoidance tag to be used by this Titan instance when allocating IDs",
            ConfigOption.Type.LOCAL, 0);
//    public static final String IDAUTHORITY_UNIQUE_ID_KEY = "idauthority-uniqueid";
//    public static final int IDAUTHORITY_UNIQUE_ID_DEFAULT = 0;


    // ############## External Index ######################
    // ################################################

    public static final String INDEX_NAMESPACE = "index";

    public static final ConfigNamespace INDEX_NS = new ConfigNamespace(ROOT_NS,"index","Configuration options for the individual indexing backends",true);


    /**
     * Define the indexing backed to use for index support
     */
    public static final ConfigOption<String> INDEX_BACKEND = new ConfigOption<String>(INDEX_NS,"backend",
            "Define the indexing backed to use for index support",
            ConfigOption.Type.GLOBAL_OFFLINE, "elasticsearch");
//    public static final String INDEX_BACKEND_KEY = "backend";
//    public static final String INDEX_BACKEND_DEFAULT = "lucene";

    public static final ConfigOption<String> INDEX_DIRECTORY = new ConfigOption<String>(INDEX_NS,"directory",
            "Directory to store index data locally",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);

    public static final ConfigOption<String> INDEX_NAME = new ConfigOption<String>(INDEX_NS,"index-name",
            "Name of the index if required by the indexing backend",
            ConfigOption.Type.GLOBAL_OFFLINE, "titan");

    public static final ConfigOption<String[]> INDEX_HOSTS = new ConfigOption<String[]>(INDEX_NS,"hostname",
            "Hostname of the indexing backend",
            ConfigOption.Type.GLOBAL, new String[]{NetworkUtil.getLoopbackAddress()});

    public static final ConfigOption<Integer> INDEX_PORT = new ConfigOption<Integer>(INDEX_NS,"port",
            "Configuration key for the port on which to connect to remote indexing backend servers",
            ConfigOption.Type.MASKABLE, Integer.class);

    public static final ConfigOption<String> INDEX_CONF_FILE = new ConfigOption<String>(INDEX_NS,"conffile",
            "Path to a configuration file for those indexing backends that require/support a separate config file",
            ConfigOption.Type.MASKABLE, String.class);


    // ############## Logging System ######################
    // ################################################

    public static final ConfigNamespace LOG_NS = new ConfigNamespace(GraphDatabaseConfiguration.ROOT_NS,"log","Configuration options for Titan's logging system",true);

    public static final String MANAGEMENT_LOG = "titan";
    public static final String TRANSACTION_LOG = "tx";
    public static final String TRIGGER_LOG = "trigger";

    public static final ConfigOption<String> LOG_BACKEND = new ConfigOption<String>(LOG_NS,"backend",
            "Define the log backed to use",
            ConfigOption.Type.GLOBAL_OFFLINE, "default");

    public static final ConfigOption<Integer> LOG_NUM_BUCKETS = new ConfigOption<Integer>(LOG_NS,"num-buckets",
            "The number of buckets to split log entries into for load balancing",
            ConfigOption.Type.GLOBAL_OFFLINE, 1, ConfigOption.positiveInt());

    public static final ConfigOption<Integer> LOG_SEND_BATCH_SIZE = new ConfigOption<Integer>(LOG_NS,"send-batch-size",
            "Maximum number of log messages to batch up for sending for logging implementations that support batch sending",
            ConfigOption.Type.MASKABLE, 256, ConfigOption.positiveInt());

    public static final ConfigOption<Integer> LOG_READ_BATCH_SIZE = new ConfigOption<Integer>(LOG_NS,"read-batch-size",
            "Maximum number of log messages to read at a time for logging implementations that read messages in batches",
            ConfigOption.Type.MASKABLE, 1024, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> LOG_SEND_DELAY = new ConfigOption<Duration>(LOG_NS,"send-delay",
            "Maximum time in ms that messages can be buffered locally before sending in batch",
            ConfigOption.Type.MASKABLE, Duration.class, new StandardDuration(1000L, TimeUnit.MILLISECONDS));

    public static final ConfigOption<Duration> LOG_READ_INTERVAL = new ConfigOption<Duration>(LOG_NS,"read-interval",
            "Time in ms between message readings from the backend for this logging implementations that read message in batch",
            ConfigOption.Type.MASKABLE, Duration.class, new StandardDuration(5000L, TimeUnit.MILLISECONDS));

    public static final ConfigOption<Integer> LOG_READ_THREADS = new ConfigOption<Integer>(LOG_NS,"read-threads",
            "Number of threads to be used in reading and processing log messages",
            ConfigOption.Type.MASKABLE, 1, ConfigOption.positiveInt());

    // ############## Attributes ######################
    // ################################################

    public static final String ATTRIBUTE_NAMESPACE = "attributes";

    public static final ConfigNamespace ATTRIBUTE_NS = new ConfigNamespace(ROOT_NS,"attributes","Configuration options for attribute handling");

    public static final ConfigOption<Boolean> ATTRIBUTE_ALLOW_ALL_SERIALIZABLE = new ConfigOption<Boolean>(ATTRIBUTE_NS,"allow-all",
            "Enables Titan to store any kind of attribute value in the database",
            ConfigOption.Type.GLOBAL_OFFLINE, true);
//    public static final String ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_KEY = "allow-all";
//    public static final boolean ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_DEFAULT = true;

    public static final ConfigNamespace CUSTOM_ATTRIBUTE_NS = new ConfigNamespace(ATTRIBUTE_NS,"custom","Custom attribute serialization and handling",true);

    public static final String ATTRIBUTE_PREFIX = "attribute";

    public static final ConfigOption<String> CUSTOM_ATTRIBUTE_CLASS = new ConfigOption<String>(CUSTOM_ATTRIBUTE_NS,"attribute-class",
            "Class of the custom attribute to be registered",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);
    public static final ConfigOption<String> CUSTOM_SERIALIZER_CLASS = new ConfigOption<String>(CUSTOM_ATTRIBUTE_NS,"serializer-class",
            "Class of the custom attribute serializer to be registered",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);
//    private static final String ATTRIBUTE_PREFIX = "attribute";
//    private static final String SERIALIZER_PREFIX = "serializer";

    // ################ Metrics #######################
    // ################################################

    /**
     * Configuration key prefix for Metrics.
     */
//    public static final String METRICS_NAMESPACE = "metrics";
    public static final ConfigNamespace METRICS_NS = new ConfigNamespace(ROOT_NS,"metrics","Configuration options for metrics reporting");

    /**
     * Whether to enable basic timing and operation count monitoring on backend
     * methods using the {@code com.codahale.metrics} package.
     */
    public static final ConfigOption<Boolean> BASIC_METRICS = new ConfigOption<Boolean>(METRICS_NS,"enabled",
            "Whether to enable basic timing and operation count monitoring on backend",
            ConfigOption.Type.MASKABLE, false);
//    public static final String BASIC_METRICS = "enable-basic-metrics";
//    public static final boolean BASIC_METRICS_DEFAULT = false;

    /**
     * This is the prefix used outside of a graph database configuration, or for
     * operations where a system-internal transaction is necessary as an
     * implementation detail. It currently can't be modified, though there is no
     * substantial technical obstacle preventing it from being configured --
     * some kind of configuration object is in scope everywhere it is used, and
     * it could theoretically be stored in and read from that object.
     */
    public static final String METRICS_PREFIX_DEFAULT = "com.thinkaurelius.titan";
    public static final String METRICS_SYSTEM_PREFIX_DEFAULT = METRICS_PREFIX_DEFAULT + "." + "sys";

    /**
     * The default name prefix for Metrics reported by Titan. All metric names
     * will begin with this string and a period. This value can be overridden on
     * a transaction-specific basis through
     * {@link StandardTransactionBuilder#setGroupName(String)}.
     * <p/>
     * Default = {@literal #METRICS_PREFIX_DEFAULT}
     */
    public static final ConfigOption<String> METRICS_PREFIX = new ConfigOption<String>(METRICS_NS,"prefix",
            "The default name prefix for Metrics reported by Titan.",
            ConfigOption.Type.MASKABLE, METRICS_PREFIX_DEFAULT);
//    public static final String METRICS_PREFIX_KEY = "prefix";

    /**
     * Whether to aggregate measurements for the edge store, vertex index, edge
     * index, and ID store.
     * <p/>
     * If true, then metrics for each of these backends will use the same metric
     * name ("stores"). All of their measurements will be combined. This setting
     * measures the sum of Titan's backend activity without distinguishing
     * between contributions of its various internal stores.
     * <p/>
     * If false, then metrics for each of these backends will use a unique
     * metric name ("idStore", "edgeStore", "vertexIndex", and "edgeIndex").
     * This setting exposes the activity associated with each backend component,
     * but it also multiplies the number of measurements involved by four.
     * <p/>
     * This option has no effect when {@link #BASIC_METRICS} is false.
     */
    public static final ConfigOption<Boolean> METRICS_MERGE_STORES = new ConfigOption<Boolean>(METRICS_NS,"merge-stores",
            "Whether to aggregate measurements for the edge store, vertex index, edge index, and ID store",
            ConfigOption.Type.MASKABLE, true);
//    public static final String MERGE_BASIC_METRICS_KEY = "merge-basic-metrics";
//    public static final boolean MERGE_BASIC_METRICS_DEFAULT = true;



    public static final ConfigNamespace METRICS_CONSOLE_NS = new ConfigNamespace(METRICS_NS,"console","Configuration options for metrics reporting to console");


    /**
     * Metrics console reporter interval in milliseconds. Leaving this
     * configuration key absent or null disables the console reporter.
     */
    public static final ConfigOption<Duration> METRICS_CONSOLE_INTERVAL = new ConfigOption<Duration>(METRICS_CONSOLE_NS,"interval",
            "Time between Metrics reports printing to the console, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);
//    public static final String METRICS_CONSOLE_INTERVAL_KEY = "console.interval";
//    public static final Long METRICS_CONSOLE_INTERVAL_DEFAULT = null;

    public static final ConfigNamespace METRICS_CSV_NS = new ConfigNamespace(METRICS_NS,"csv","Configuration options for metrics reporting to CSV file");

    /**
     * Metrics CSV reporter interval in milliseconds. Leaving this configuration
     * key absent or null disables the CSV reporter.
     */
    public static final ConfigOption<Duration> METRICS_CSV_INTERVAL = new ConfigOption<Duration>(METRICS_CSV_NS,"interval",
            "Time between dumps of CSV files containing Metrics data, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);
//    public static final String METRICS_CSV_INTERVAL_KEY = "csv.interval";
//    public static final Long METRICS_CSV_INTERVAL_DEFAULT = null;

    /**
     * Metrics CSV output directory. It will be created if it doesn't already
     * exist. This option must be non-null if {@link #METRICS_CSV_INTERVAL} is
     * non-null. This option has no effect if {@code #METRICS_CSV_INTERVAL} is
     * null.
     */
    public static final ConfigOption<String> METRICS_CSV_DIR = new ConfigOption<String>(METRICS_CSV_NS,"directory",
            "Metrics CSV output directory",
            ConfigOption.Type.MASKABLE, String.class);

//    public static final String METRICS_CSV_DIR_KEY = "csv.dir";
//    public static final String METRICS_CSV_DIR_DEFAULT = null;

    public static final ConfigNamespace METRICS_JMX_NS = new ConfigNamespace(METRICS_NS,"jmx","Configuration options for metrics reporting through JMX");

    /**
     * Whether to report Metrics through a JMX MBean.
     */
    public static final ConfigOption<Boolean> METRICS_JMX_ENABLED = new ConfigOption<Boolean>(METRICS_JMX_NS,"enabled",
            "Whether to report Metrics through a JMX MBean",
            ConfigOption.Type.MASKABLE, false);
//    public static final String METRICS_JMX_ENABLED_KEY = "jmx.enabled";
//    public static final boolean METRICS_JMX_ENABLED_DEFAULT = false;

    /**
     * The JMX domain in which to report Metrics. If null, then Metrics applies
     * its default value.
     */
    public static final ConfigOption<String> METRICS_JMX_DOMAIN = new ConfigOption<String>(METRICS_JMX_NS,"domain",
            "The JMX domain in which to report Metrics",
            ConfigOption.Type.MASKABLE, String.class);
//    public static final String METRICS_JMX_DOMAIN_KEY = "jmx.domain";
//    public static final String METRICS_JMX_DOMAIN_DEFAULT = null;

    /**
     * The JMX agentId through which to report Metrics. Calling
     * {@link MBeanServerFactory#findMBeanServer(String)} on this value must
     * return exactly one {@code MBeanServer} at runtime. If null, then Metrics
     * applies its default value.
     */
    public static final ConfigOption<String> METRICS_JMX_AGENTID = new ConfigOption<String>(METRICS_JMX_NS,"agentid",
            "The JMX agentId used by Metrics",
            ConfigOption.Type.MASKABLE, String.class);

//    public static final String METRICS_JMX_AGENTID_KEY = "jmx.agentid";
//    public static final String METRICS_JMX_AGENTID_DEFAULT = null;

    public static final ConfigNamespace METRICS_SLF4J_NS = new ConfigNamespace(METRICS_NS,"slf4j","Configuration options for metrics reporting through slf4j");


    /**
     * Metrics Slf4j reporter interval in milliseconds. Leaving this
     * configuration key absent or null disables the Slf4j reporter.
     */
    public static final ConfigOption<Duration> METRICS_SLF4J_INTERVAL = new ConfigOption<Duration>(METRICS_SLF4J_NS,"interval",
            "Time between slf4j logging reports of Metrics data, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);
//    public static final String METRICS_SLF4J_INTERVAL_KEY = "slf4j.interval";
//    public static final Long METRICS_SLF4J_INTERVAL_DEFAULT = null;

    /**
     * The complete name of the Logger through which Metrics will report via
     * Slf4j. If non-null, then Metrics will be dumped on
     * {@link LoggerFactory#getLogger(String)} with the configured value as the
     * argument. If null, then Metrics will use its default Slf4j logger.
     */
    public static final ConfigOption<String> METRICS_SLF4J_LOGGER = new ConfigOption<String>(METRICS_SLF4J_NS,"logger",
            "The complete name of the Logger through which Metrics will report via Slf4j",
            ConfigOption.Type.MASKABLE, String.class);

//    public static final String METRICS_SLF4J_LOGGER_KEY = "slf4j.logger";
//    public static final String METRICS_SLF4J_LOGGER_DEFAULT = null;

    /**
     * The configuration namespace within {@link #METRICS_NS} for Ganglia.
     */
    public static final ConfigNamespace METRICS_GANGLIA_NS = new ConfigNamespace(METRICS_NS,"ganglia","Configuration options for metrics reporting through Ganglia");

    /**
     * The unicast host or multicast group name to which Metrics will send
     * Ganglia data. Setting this config key has no effect unless
     * {@link #GANGLIA_INTERVAL} is also set.
     */
    public static final ConfigOption<String> GANGLIA_HOST_OR_GROUP = new ConfigOption<String>(METRICS_GANGLIA_NS,"hostname",
            "The unicast host or multicast group name to which Metrics will send Ganglia data",
            ConfigOption.Type.MASKABLE, String.class);
//
//    public static final String GANGLIA_HOST_OR_GROUP_KEY = "hostname";

    /**
     * The number of milliseconds to wait between sending Metrics data to the
     * host or group specified by {@link #GANGLIA_HOST_OR_GROUP}. This has no
     * effect unless {@link #GANGLIA_HOST_OR_GROUP} is also set.
     */
    public static final ConfigOption<Duration> GANGLIA_INTERVAL = new ConfigOption<Duration>(METRICS_GANGLIA_NS,"interval",
            "The number of milliseconds to wait between sending Metrics data to Ganglia",
            ConfigOption.Type.MASKABLE, Duration.class);

//    public static final String GANGLIA_INTERVAL_KEY = "interval";

    /**
     * The port to which Ganglia data are sent.
     * <p/>
     */
    public static final ConfigOption<Integer> GANGLIA_PORT = new ConfigOption<Integer>(METRICS_GANGLIA_NS,"port",
            "The port to which Ganglia data are sent",
            ConfigOption.Type.MASKABLE, 8649);
//    public static final String GANGLIA_PORT = "port";
//    public static final int GANGLIA_PORT_DEFAULT = 8649;

    /**
     * Whether to interpret {@link #GANGLIA_HOST_OR_GROUP} as a unicast or
     * multicast address. If present, it must be either the string "multicast"
     * or the string "unicast".
     * <p/>
     */
    public static final ConfigOption<String> GANGLIA_ADDRESSING_MODE = new ConfigOption<String>(METRICS_GANGLIA_NS,"addressing-mode",
            "Whether to communicate to Ganglia via uni- or multicast",
            ConfigOption.Type.MASKABLE, "unicast", new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
            return s!=null && s.equalsIgnoreCase("unicast") || s.equalsIgnoreCase("multicast");
        }
    });

//    public static final String GANGLIA_ADDRESSING_MODE_KEY = "addressing-mode";
//    public static final String GANGLIA_ADDRESSING_MODE_DEFAULT = "unicast";

    /**
     * The multicast TTL to set on outgoing Ganglia datagrams. This has no
     * effect when {@link #GANGLIA_ADDRESSING_MODE} is set to "multicast".
     * <p/>
     * This is a TTL in the multicast protocol sense (number of routed hops),
     * not a timestamp sense.
     */
    public static final ConfigOption<Integer> GANGLIA_TTL = new ConfigOption<Integer>(METRICS_GANGLIA_NS,"ttl",
            "The multicast TTL to set on outgoing Ganglia datagrams",
            ConfigOption.Type.MASKABLE, 1);

//    public static final String GANGLIA_TTL_KEY = "ttl";
//    public static final int GANGLIA_TTL_DEFAULT = 1;

    /**
     * Whether to send data to Ganglia in the 3.1 protocol format (true) or the
     * 3.0 protocol format (false).
     * <p/>
     */
    public static final ConfigOption<Boolean> GANGLIA_USE_PROTOCOL_31 = new ConfigOption<Boolean>(METRICS_GANGLIA_NS,"protocol-31",
            "Whether to send data to Ganglia in the 3.1 protocol format",
            ConfigOption.Type.MASKABLE, true);
//    public static final String GANGLIA_USE_PROTOCOL_31_KEY = "protocol-31";
//    public static final boolean GANGLIA_USE_PROTOCOL_31_DEFAULT = true;

    /**
     * The host UUID to set on outgoing Ganglia datagrams. If null, no UUID is
     * set on outgoing data.
     * <p/>
     * See https://github.com/ganglia/monitor-core/wiki/UUIDSources
     * <p/>
     */
    public static final ConfigOption<String> GANGLIA_UUID = new ConfigOption<String>(METRICS_GANGLIA_NS,"uuid",
            "The host UUID to set on outgoing Ganglia datagrams. " +
            "See https://github.com/ganglia/monitor-core/wiki/UUIDSources for information about this setting.",
            ConfigOption.Type.LOCAL, String.class);
//    public static final String GANGLIA_UUID_KEY = "uuid";
//    public static final UUID GANGLIA_UUID_DEFAULT = null;

    /**
     * If non-null, it must be a valid Gmetric spoof string formatted as an
     * IP:hostname pair. If null, Ganglia will automatically determine the IP
     * and hostname to set on outgoing datagrams.
     * <p/>
     * See http://sourceforge.net/apps/trac/ganglia/wiki/gmetric_spoofing
     * <p/>
     */
    public static final ConfigOption<String> GANGLIA_SPOOF = new ConfigOption<String>(METRICS_GANGLIA_NS,"spoof",
            "If non-null, it must be a valid Gmetric spoof string formatted as an IP:hostname pair. " +
            "See http://sourceforge.net/apps/trac/ganglia/wiki/gmetric_spoofing for information about this setting.",
            ConfigOption.Type.MASKABLE, String.class, new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
            return s!=null && 0 < s.indexOf(':');
        }
    });
//    public static final String GANGLIA_SPOOF_KEY = "spoof";
//    public static final String GANGLIA_SPOOF_DEFAULT = null;

    /**
     * The configuration namespace within {@link #METRICS_NS} for
     * Graphite.
     */
    public static final ConfigNamespace METRICS_GRAPHITE_NS = new ConfigNamespace(METRICS_NS,"graphite","Configuration options for metrics reporting through Graphite");

//    public static final String GRAPHITE_NAMESPACE = "graphite";

    /**
     * The hostname to receive Graphite plaintext protocol metric data. Setting
     * this config key has no effect unless {@link #GRAPHITE_INTERVAL} is also
     * set.
     */
    public static final ConfigOption<String> GRAPHITE_HOST = new ConfigOption<String>(METRICS_GRAPHITE_NS,"hostname",
            "The hostname to receive Graphite plaintext protocol metric data",
            ConfigOption.Type.MASKABLE, String.class);
//    public static final String GRAPHITE_HOST_KEY = "hostname";

    /**
     * The number of milliseconds to wait between sending Metrics data to the
     * host specified {@link #GRAPHITE_HOST}. This has no effect unless
     * {@link #GRAPHITE_HOST} is also set.
     */
    public static final ConfigOption<Duration> GRAPHITE_INTERVAL = new ConfigOption<Duration>(METRICS_GRAPHITE_NS,"interval",
            "The number of milliseconds to wait between sending Metrics data",
            ConfigOption.Type.MASKABLE, Duration.class);
//    public static final String GRAPHITE_INTERVAL_KEY = "interval";

    /**
     * The port to which Graphite data are sent.
     * <p/>
     */
    public static final ConfigOption<Integer> GRAPHITE_PORT = new ConfigOption<Integer>(METRICS_GRAPHITE_NS,"port",
            "The port to which Graphite data are sent",
            ConfigOption.Type.MASKABLE, 2003);
//    public static final String GRAPHITE_PORT_KEY = "port";
//    public static final int GRAPHITE_PORT_DEFAULT = 2003;

    /**
     * A Graphite-specific prefix for reported metrics. If non-null, Metrics
     * prepends this and a "." to all metric names before reporting them to
     * Graphite.
     * <p/>
     */
    public static final ConfigOption<String> GRAPHITE_PREFIX = new ConfigOption<String>(METRICS_GRAPHITE_NS,"prefix",
            "A Graphite-specific prefix for reported metrics",
            ConfigOption.Type.MASKABLE, String.class);
//    public static final String GRAPHITE_PREFIX_KEY = "prefix";
//    public static final String GRAPHITE_PREFIX_DEFAULT = null;


    // ################ Begin Class Definition #######################
    // ###############################################################

    public static final String SYSTEM_PROPERTIES_STORE_NAME = "system_properties";
    public static final String SYSTEM_CONFIGURATION_IDENTIFIER = "configuration";


    private final Configuration configuration;
    private final String uniqueGraphId;


    private boolean readOnly;
    private boolean flushIDs;
    private boolean forceIndexUsage;
    private boolean batchLoading;
    private int txVertexCacheSize;
    private int txDirtyVertexSize;
    private DefaultSchemaMaker defaultSchemaMaker;
    private Boolean propertyPrefetching;
    private boolean allowVertexIdSetting;
    private boolean logTransactions;
    private String metricsPrefix;
    private String unknownIndexKeyName;

    private StoreFeatures storeFeatures = null;

    public GraphDatabaseConfiguration(ReadConfiguration localConfig) {
        Preconditions.checkNotNull(localConfig);

        BasicConfiguration localbc = new BasicConfiguration(ROOT_NS,localConfig, BasicConfiguration.Restriction.NONE);
        ModifiableConfiguration overwrite = new ModifiableConfiguration(ROOT_NS,new CommonsConfiguration(), BasicConfiguration.Restriction.NONE);

//        KeyColumnValueStoreManager storeManager=null;
        final KeyColumnValueStoreManager storeManager = Backend.getStorageManager(localbc);
        KCVSConfiguration kcvsConfig=Backend.getStandaloneGlobalConfiguration(storeManager,localbc);
        ReadConfiguration globalConfig=null;

        //Read out global configuration
        try {
            // If lock prefix is unspecified, specify it now
            if (!localbc.has(ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX)) {
                overwrite.set(ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX, storeManager.getName());
            }

            //Freeze global configuration if not already frozen!
            ModifiableConfiguration globalWrite = new ModifiableConfiguration(ROOT_NS,kcvsConfig, BasicConfiguration.Restriction.GLOBAL);
            if (!globalWrite.isFrozen()) {
                //Copy over global configurations
                Map<ConfigElement.PathIdentifier,Object> allOptions = localbc.getAll();
                globalWrite.setAll(Maps.filterEntries(allOptions,new Predicate<Map.Entry<ConfigElement.PathIdentifier, Object>>() {
                    @Override
                    public boolean apply(@Nullable Map.Entry<ConfigElement.PathIdentifier, Object> entry) {
                        assert entry.getKey().element.isOption();
                        return ((ConfigOption)entry.getKey().element).isGlobal();
                    }
                }));

                //Write Titan version
                Preconditions.checkArgument(!globalWrite.has(INITIAL_TITAN_VERSION),"Database has already been initialized but not frozen");
                globalWrite.set(INITIAL_TITAN_VERSION,TitanConstants.VERSION);

                // If partitioning is unspecified, specify it now
                if (!localbc.has(CLUSTER_PARTITION)) {
                    StoreFeatures f = storeManager.getFeatures();
                    boolean part = f.isDistributed() && f.isKeyOrdered();
                    globalWrite.set(CLUSTER_PARTITION, part);
                    log.info("Enabled partitioning", part);
                } else {
                    log.info("Disabled partitioning");
                }

                globalWrite.freezeConfiguration();
            } else {
                String version = globalWrite.get(INITIAL_TITAN_VERSION);
                Preconditions.checkArgument(version!=null,"Titan version has not been initialized");
                if (!TitanConstants.VERSION.equals(version) && !TitanConstants.COMPATIBLE_VERSIONS.contains(version)) {
                    throw new TitanException("StorageBackend version is incompatible with current Titan version: " + version + " vs. " + TitanConstants.VERSION);
                }
            }

            globalConfig = kcvsConfig.asReadConfiguration();
        } finally {
            kcvsConfig.close();
        }
        Configuration combinedConfig = new MixedConfiguration(ROOT_NS,globalConfig,localConfig);

        //Compute unique instance id
        this.uniqueGraphId = getOrGenerateUniqueInstanceId(combinedConfig);
        overwrite.set(UNIQUE_INSTANCE_ID, this.uniqueGraphId);

        //Default log configuration for system and tx log
        //TRANSACTION LOG: send_delay=0 for tx log
        Preconditions.checkArgument(!combinedConfig.has(LOG_SEND_DELAY,TRANSACTION_LOG) ||
                combinedConfig.get(LOG_SEND_DELAY, TRANSACTION_LOG).isZeroLength(),"Send delay must be 0 for transaction log.");
        overwrite.set(LOG_SEND_DELAY, ZeroDuration.INSTANCE,TRANSACTION_LOG);
        //SYSTEM MANAGEMENT LOG: backend=default and send_delay=0 and key_consistent=true and fixed-partitions=true
        Preconditions.checkArgument(combinedConfig.get(LOG_BACKEND,MANAGEMENT_LOG).equals(LOG_BACKEND.getDefaultValue()),
                "Must use default log backend for system log");
        Preconditions.checkArgument(!combinedConfig.has(LOG_SEND_DELAY,MANAGEMENT_LOG) ||
                combinedConfig.get(LOG_SEND_DELAY,MANAGEMENT_LOG).isZeroLength(),"Send delay must be 0 for system log.");
        overwrite.set(LOG_SEND_DELAY, ZeroDuration.INSTANCE, MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(KCVSLog.LOG_KEY_CONSISTENT, MANAGEMENT_LOG) ||
                combinedConfig.get(KCVSLog.LOG_KEY_CONSISTENT, MANAGEMENT_LOG), "Management log must be configured to be key-consistent");
        overwrite.set(KCVSLog.LOG_KEY_CONSISTENT,true,MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(KCVSLogManager.LOG_FIXED_PARTITION,MANAGEMENT_LOG)
                || combinedConfig.get(KCVSLogManager.LOG_FIXED_PARTITION,MANAGEMENT_LOG),"Fixed partitions must be enabled for management log");
        overwrite.set(KCVSLogManager.LOG_FIXED_PARTITION,true,MANAGEMENT_LOG);

        this.configuration = new MergedConfiguration(overwrite,combinedConfig);
        preLoadConfiguration();
    }

    private static final AtomicLong INSTANCE_COUNTER = new AtomicLong(0);

    private static String computeUniqueInstanceId(Configuration config) {
        final String suffix;

        if (config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX)) {
            suffix = LongEncoding.encode(config.get(
                    GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX));
        } else {
            suffix = ManagementFactory.getRuntimeMXBean().getName() + LongEncoding.encode(INSTANCE_COUNTER.incrementAndGet());
        }

        byte[] addrBytes;
        try {
            addrBytes = Inet4Address.getLocalHost().getAddress();
        } catch (UnknownHostException e) {
            throw new TitanConfigurationException("Cannot determine local host", e);
        }
        String uid = new String(Hex.encodeHex(addrBytes)) + suffix;
        for (char c : ConfigElement.ILLEGAL_CHARS) {
            uid = StringUtils.replaceChars(uid,c,'-');
        }
        return uid;
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

    public static final ModifiableConfiguration buildConfiguration() {
        return new ModifiableConfiguration(ROOT_NS,
                new CommonsConfiguration(new BaseConfiguration()),
                BasicConfiguration.Restriction.NONE);
    }


    public static final String getSystemMetricsPrefix() {
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
        defaultSchemaMaker = preregisteredAutoType.get(configuration.get(AUTO_TYPE));
        //Disable auto-type making when batch-loading is enabled since that may overwrite types without warning
        if (batchLoading) defaultSchemaMaker = DisableDefaultSchemaMaker.INSTANCE;

        txVertexCacheSize = configuration.get(TX_CACHE_SIZE);
        //Check for explicit dirty vertex cache size first, then fall back on batch-loading-dependent default
        if (configuration.has(TX_DIRTY_SIZE)) {
            txDirtyVertexSize = configuration.get(TX_DIRTY_SIZE);
        } else {
            txDirtyVertexSize = batchLoading ?
                    TX_DIRTY_SIZE_DEFAULT_WITH_BATCH :
                    TX_DIRTY_SIZE_DEFAULT_WITHOUT_BATCH;
        }

        if (configuration.has(PROPERTY_PREFETCHING))
            propertyPrefetching = configuration.get(PROPERTY_PREFETCHING);
        else propertyPrefetching = null;
        allowVertexIdSetting = configuration.get(ALLOW_SETTING_VERTEX_ID);
        logTransactions = configuration.get(SYSTEM_LOG_TRANSACTIONS);

        unknownIndexKeyName = configuration.get(IGNORE_UNKNOWN_INDEX_FIELD) ? UKNOWN_FIELD_NAME : null;

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
            final Duration ival = configuration.get(GANGLIA_INTERVAL);
            final Integer port = configuration.get(GANGLIA_PORT);

            final UDPAddressingMode addrMode;
            final String addrModeStr = configuration.get(GANGLIA_ADDRESSING_MODE);
            if (addrModeStr.equalsIgnoreCase("multicast")) {
                addrMode = UDPAddressingMode.MULTICAST;
            } else if (addrModeStr.equalsIgnoreCase("unicast")) {
                addrMode = UDPAddressingMode.UNICAST;
            } else throw new AssertionError();

            final Boolean proto31 = configuration.get(GANGLIA_USE_PROTOCOL_31);

            final int ttl = configuration.get(GANGLIA_TTL);

            final UUID uuid = configuration.has(GANGLIA_UUID)? UUID.fromString(configuration.get(GANGLIA_UUID)):null;

            String spoof = null;
            if (configuration.has(GANGLIA_SPOOF)) spoof = configuration.get(GANGLIA_SPOOF);

            try {
                MetricManager.INSTANCE.addGangliaReporter(host, port, addrMode, ttl, proto31, uuid, spoof, ival);
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

    public boolean allowVertexIdSetting() {
        return allowVertexIdSetting;
    }

    public boolean hasPropertyPrefetching() {
        if (propertyPrefetching == null) {
            return getStoreFeatures().isDistributed();
        } else {
            return propertyPrefetching;
        }
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
        List<RegisteredAttributeClass<?>> all = new ArrayList<RegisteredAttributeClass<?>>();
        for (String attributeId : configuration.getContainedNamespaces(CUSTOM_ATTRIBUTE_NS)) {
            Preconditions.checkArgument(attributeId.startsWith(ATTRIBUTE_PREFIX),"Invalid attribute definition: %s",attributeId);
            int position;
            try {
                position = Integer.parseInt(attributeId.substring(ATTRIBUTE_PREFIX.length()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Expected entry of the form ["+ ATTRIBUTE_PREFIX +"X] where X is a number but given" + attributeId);
            }
            Class<?> clazz = null;
            AttributeHandler<?> serializer = null;
            String classname = configuration.get(CUSTOM_ATTRIBUTE_CLASS,attributeId);
            try {
                clazz = Class.forName(classname);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not find attribute class" + classname, e);
            }
            Preconditions.checkNotNull(clazz);

            Preconditions.checkArgument(configuration.has(CUSTOM_SERIALIZER_CLASS, attributeId));
            String serializername = configuration.get(CUSTOM_SERIALIZER_CLASS, attributeId);
            try {
                Class sclass = Class.forName(serializername);
                serializer = (AttributeHandler) sclass.newInstance();
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not find serializer class" + serializername);
            } catch (InstantiationException e) {
                throw new IllegalArgumentException("Could not instantiate serializer class" + serializername, e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Could not instantiate serializer class" + serializername, e);
            }
            Preconditions.checkNotNull(serializer);
            RegisteredAttributeClass reg = new RegisteredAttributeClass(clazz, serializer);
            for (int i = 0; i < all.size(); i++) {
                if (all.get(i).equals(reg)) {
                    throw new IllegalArgumentException("Duplicate attribute registration: " + all.get(i) + " and " + reg);
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
        String clazzname = configuration.get(STORAGE_BACKEND);
        if (clazzname.equalsIgnoreCase("berkeleyje")) {
            return clazzname + ":" + configuration.get(STORAGE_DIRECTORY);
        } else {
            return clazzname + ":" + Arrays.toString(configuration.get(STORAGE_HOSTS));
        }
    }

    public Backend getBackend() {
        Backend backend = new Backend(configuration);
        backend.initialize(configuration);
        storeFeatures = backend.getStoreFeatures();
        return backend;
    }

    public StoreFeatures getStoreFeatures() {
        Preconditions.checkArgument(storeFeatures != null, "Cannot retrieve store features before the storage backend has been initialized");
        return storeFeatures;
    }

    public Serializer getSerializer() {
        return getSerializer(configuration);
    }


    public static Serializer getSerializer(Configuration configuration) {
        Serializer serializer = new StandardSerializer(configuration.get(ATTRIBUTE_ALLOW_ALL_SERIALIZABLE));
        for (RegisteredAttributeClass<?> clazz : getRegisteredAttributeClasses(configuration)) {
            clazz.registerWith(serializer);
        }
        return serializer;
    }

    public boolean hasSerializeAll() {
        return configuration.get(ATTRIBUTE_ALLOW_ALL_SERIALIZABLE);
    }

    public SchemaCache getTypeCache(SchemaCache.StoreRetrieval retriever) {
        if (configuration.get(BASIC_METRICS)) return new MetricInstrumentedSchemaCache(retriever);
        else return new StandardSchemaCache(retriever);
    }



	/* ----------------------------------------
     Methods for writing/reading config files
	-------------------------------------------*/

    /**
     * Returns the home directory for the graph database initialized in this configuration
     *
     * @return Home directory for this graph database configuration
     */
    public File getHomeDirectory() {
        if (!configuration.has(STORAGE_DIRECTORY))
            throw new UnsupportedOperationException("No home directory specified");
        File dir = new File(configuration.get(STORAGE_DIRECTORY));
        Preconditions.checkArgument(dir.isDirectory(), "Not a directory");
        return dir;
    }

    //TODO: which of the following methods are really needed

    /**
     * Returns the home directory path for the graph database initialized in this configuration
     *
     * @return Home directory path for this graph database configuration
     */
    public String getHomePath() {
        return getPath(getHomeDirectory());
    }

    private static File getSubDirectory(String base, String sub) {
        File subdir = new File(base, sub);
        if (!subdir.exists()) {
            if (!subdir.mkdir()) {
                throw new IllegalArgumentException("Cannot create subdirectory: " + sub);
            }
        }
        assert subdir.exists() && subdir.isDirectory();
        return subdir;
    }

    private static String getFileName(String dir, String file) {
        if (!dir.endsWith(File.separator)) dir = dir + File.separator;
        return dir + file;
    }

    public static String getPath(File dir) {
        return dir.getAbsolutePath() + File.separator;
    }


    static boolean existsFile(String file) {
        return (new File(file)).isFile();
    }

//    static PropertiesConfiguration getPropertiesConfig(String file) {
//        PropertiesConfiguration config = new PropertiesConfiguration();
//        if (existsFile(file)) {
//            try {
//                config.load(file);
//            } catch (ConfigurationException e) {
//                throw new IllegalArgumentException("Cannot load existing configuration file", e);
//            }
//        }
//        config.setFileName(file);
//        config.setAutoSave(true);
//        return config;
//    }

}
