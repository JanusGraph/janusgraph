package com.thinkaurelius.titan.graphdb.configuration;

import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.management.MBeanServerFactory;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.graphdb.types.DisableDefaultTypeMaker;
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

    // ################ GENERAL #######################
    // ################################################

    /**
     * Configures the {@link DefaultTypeMaker} to be used by this graph. If left empty, automatic creation of types
     * is disabled.
     */
    public static final String AUTO_TYPE_KEY = "autotype";
    public static final String AUTO_TYPE_DEFAULT = "blueprints";

    private static final Map<String, DefaultTypeMaker> preregisteredAutoType = new HashMap<String, DefaultTypeMaker>() {{
        put("none", DisableDefaultTypeMaker.INSTANCE);
        put("blueprints", BlueprintsDefaultTypeMaker.INSTANCE);
    }};

    /**
     * Configures the cache size used by individual transactions opened against this graph. The smaller the cache size, the
     * less memory a transaction can consume at maximum. For many concurrent, long running transactions in memory constraint
     * environments, reducing the cache size can avoid OutOfMemory and GC limit exceeded exceptions.
     * Note, however, that all modifications in a transaction must always be kept in memory and hence this setting does not
     * have much impact on write intense transactions. Those must be split into smaller transactions in the case of memory errors.
     */
    public static final String TX_CACHE_SIZE_KEY = "tx-cache-size";
    public static final long TX_CACHE_SIZE_DEFAULT = 20000;


    // ################ STORAGE #######################
    // ################################################

    public static final String STORAGE_NAMESPACE = "storage";

    /**
     * Storage directory for those storage backends that require local storage
     */
    public static final String STORAGE_DIRECTORY_KEY = "directory";

    /**
     * Define the storage backed to use for persistence
     */
    public static final String STORAGE_BACKEND_KEY = "backend";
    public static final String STORAGE_BACKEND_DEFAULT = "local";

    /**
     * Specifies whether write operations are supported
     */
    public static final String STORAGE_READONLY_KEY = "read-only";
    public static final boolean STORAGE_READONLY_DEFAULT = false;

    /**
     * Enables batch loading which improves write performance but assumes that only one thread is interacting with
     * the graph
     */
    public static final String STORAGE_BATCH_KEY = "batch-loading";
    public static final boolean STORAGE_BATCH_DEFAULT = false;

    /**
     * Enables transactions on storage backends that support them
     */
    public static final String STORAGE_TRANSACTIONAL_KEY = "transactions";
    public static final boolean STORAGE_TRANSACTIONAL_DEFAULT = true;

    /**
     * Buffers graph mutations locally up to the specified number before persisting them against the storage backend.
     * Set to 0 to disable buffering. Buffering is disabled automatically if the storage backend does not support buffered mutations.
     */
    public static final String BUFFER_SIZE_KEY = "buffer-size";
    public static final int BUFFER_SIZE_DEFAULT = 1024;

    /**
     * Number of times the database attempts to persist the transactional state to the storage layer.
     * Persisting the state of a committed transaction might fail for various reasons, some of which are
     * temporary such as network failures. For temporary failures, Titan will re-attempt to persist the
     * state up to the number of times specified.
     */
    public static final String WRITE_ATTEMPTS_KEY = "write-attempts";
    public static final int WRITE_ATTEMPTS_DEFAULT = 5;

    /**
     * Number of times the database attempts to execute a read operation against the storage layer in the current transaction.
     * A read operation might fail for various reasons, some of which are
     * temporary such as network failures. For temporary failures, Titan will re-attempt to read the
     * state up to the number of times specified before failing the transaction
     */
    public static final String READ_ATTEMPTS_KEY = "read-attempts";
    public static final int READ_ATTEMPTS_DEFAULT = 3;

    /**
     * Time in milliseconds that Titan waits after an unsuccessful storage attempt before retrying.
     */
    public static final String STORAGE_ATTEMPT_WAITTIME_KEY = "attempt-wait";
    public static final int STORAGE_ATTEMPT_WAITTIME_DEFAULT = 250;
    /**
     * A unique identifier for the machine running the @TitanGraph@ instance.
     * It must be ensured that no other machine accessing the storage backend can have the same identifier.
     */
    public static final String INSTANCE_RID_RAW_KEY = "machine-id";
    /**
     * A locally unique identifier for a particular @TitanGraph@ instance. This only needs to be configured
     * when multiple @TitanGraph@ instances are running on the same machine. A unique machine specific appendix
     * guarantees a globally unique identifier.
     */
    public static final String INSTANCE_RID_SHORT_KEY = "machine-id-appendix";

    /**
     * Number of times the system attempts to acquire a lock before giving up and throwing an exception.
     */
    public static final String LOCK_RETRY_COUNT = "lock-retries";
    public static final int LOCK_RETRY_COUNT_DEFAULT = 3;
    /**
     * The number of milliseconds the system waits for a lock application to be acknowledged by the storage backend.
     * Also, the time waited at the end of all lock applications before verifying that the applications were successful.
     * This value should be a small multiple of the average consistent write time.
     */
    public static final String LOCK_WAIT_MS = "lock-wait-time";
    public static final long LOCK_WAIT_MS_DEFAULT = 100;
    /**
     * Number of milliseconds after which a lock is considered to have expired. Lock applications that were not released
     * are considered expired after this time and released.
     * This value should be larger than the maximum time a transaction can take in order to guarantee that no correctly
     * held applications are expired pre-maturely and as small as possible to avoid dead lock.
     */
    public static final String LOCK_EXPIRE_MS = "lock-expiry-time";
    public static final long LOCK_EXPIRE_MS_DEFAULT = 300 * 1000;

    /**
     * Locker type to use.  The supported types are in {@link com.thinkaurelius.titan.diskstorage.Backend}.
     */
    public static final String LOCK_BACKEND = "lock-backend";
    public static final String LOCK_BACKEND_DEFAULT = "consistentkey";

    /**
     * The number of milliseconds the system waits for an id block application to be acknowledged by the storage backend.
     * Also, the time waited after the application before verifying that the application was successful.
     */
    public static final String IDAUTHORITY_WAIT_MS_KEY = "idauthority-wait-time";
    public static final long IDAUTHORITY_WAIT_MS_DEFAULT = 300;
    /**
     * Number of times the system attempts to acquire a unique id block before giving up and throwing an exception.
     */
    public static final String IDAUTHORITY_RETRY_COUNT_KEY = "idauthority-retries";
    public static final int IDAUTHORITY_RETRY_COUNT_DEFAULT = 20;

    /**
     * Configuration key for the hostname or list of hostname of remote storage backend servers to connect to.
     * <p/>
     * Value = {@value}
     */
    public static final String HOSTNAME_KEY = "hostname";
    /**
     * Default hostname at which to attempt connecting to remote storage backend
     * <p/>
     * Value = {@value}
     */
    public static final String HOSTNAME_DEFAULT = "127.0.0.1";
    /**
     * Configuration key for the port on which to connect to remote storage backend servers.
     * <p/>
     * Value = {@value}
     */
    public static final String PORT_KEY = "port";
    /**
     * Default timeout whne connecting to a remote database instance
     * <p/>
     * Value = {@value}
     */
    public static final int CONNECTION_TIMEOUT_DEFAULT = 10000;
    public static final String CONNECTION_TIMEOUT_KEY = "connection-timeout";

    /**
     * Time in milliseconds for backend manager to wait for the storage backends to
     * become available when Titan is run in server mode. Should the backend manager
     * experience exceptions when attempting to access the storage backend it will retry
     * until this timeout is exceeded.
     * <p/>
     * A wait time of 0 disables waiting.
     * <p/>
     * Value = {@value}
     */
    public static final int SETUP_WAITTIME_DEFAULT = 60000;
    public static final String SETUP_WAITTIME_KEY = "setup-wait";

    /**
     * Default number of connections to pool when connecting to a remote database.
     * <p/>
     * Value = {@value}
     */
    public static final int CONNECTION_POOL_SIZE_DEFAULT = 32;
    public static final String CONNECTION_POOL_SIZE_KEY = "connection-pool-size";

    /**
     * Default number of results to pull over the wire when iterating over a distributed
     * storage backend.
     * This is batch size of results to pull when iterating a result set.
     */
    public static final int PAGE_SIZE_DEFAULT = 100;
    public static final String PAGE_SIZE_KEY = "page-size";

    // ################ IDS ###########################
    // ################################################

    public static final String IDS_NAMESPACE = "ids";

    /**
     * Size of the block to be acquired. Larger block sizes require fewer block applications but also leave a larger
     * fraction of the id pool occupied and potentially lost. For write heavy applications, larger block sizes should
     * be chosen.
     */
    public static final String IDS_BLOCK_SIZE_KEY = "block-size";
    public static final int IDS_BLOCK_SIZE_DEFAULT = 10000;

    /**
     * Whether the id space should be partitioned for equal distribution of keys. If the keyspace is ordered, this needs to be
     * enabled to ensure an even distribution of data. If the keyspace is random/hashed, then enabling this only has the benefit
     * of de-congesting a single id pool in the database.
     */
    public static final String IDS_PARTITION_KEY = "partition";
    public static final boolean IDS_PARTITION_DEFAULT = false;

    /**
     * If flush ids is enabled, vertices and edges are assigned ids immediately upon creation. If not, then ids are only
     * assigned when the transaction is committed.
     */
    public static final String IDS_FLUSH_KEY = "flush";
    public static final boolean IDS_FLUSH_DEFAULT = true;

    /**
     * The number of milliseconds that the Titan id pool manager will wait before giving up on allocating a new block
     * of ids. Note, that failure to allocate a new id block will cause the entire database to fail, hence this value
     * should be set conservatively. Choose a high value if there is a lot of contention around id allocation.
     */
    public static final String IDS_RENEW_TIMEOUT_KEY = "renew-timeout";
    public static final long IDS_RENEW_TIMEOUT_DEFAULT = 60 * 1000; // 1 minute

    /**
     * Configures when the id pool manager will attempt to allocate a new id block. When all but the configured percentage
     * of the current block is consumed, a new block will be allocated. Larger values should be used if a lot of ids
     * are allocated in a short amount of time. Value must be in (0,1].
     */
    public static final String IDS_RENEW_BUFFER_PERCENTAGE_KEY = "renew-percentage";
    public static final double IDS_RENEW_BUFFER_PERCENTAGE_DEFAULT = 0.3; // 30 %

    // ############## External Index ######################
    // ################################################

    public static final String INDEX_NAMESPACE = "index";


    /**
     * Define the storage backed to use for persistence
     */
    public static final String INDEX_BACKEND_KEY = "backend";
    public static final String INDEX_BACKEND_DEFAULT = "lucene";


    // ############## Attributes ######################
    // ################################################

    public static final String ATTRIBUTE_NAMESPACE = "attributes";

    public static final String ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_KEY = "allow-all";
    public static final boolean ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_DEFAULT = true;
    private static final String ATTRIBUTE_PREFIX = "attribute";
    private static final String SERIALIZER_PREFIX = "serializer";

    // ################ Metrics #######################
    // ################################################

    /**
     * Prefix for Metrics reporter configuration keys.
     */
    public static final String METRICS_NAMESPACE = "metrics";

    /**
     * Whether to enable basic timing and operation count monitoring on backend
     * methods using the {@code com.codahale.metrics} package.
     */
    public static final String BASIC_METRICS = "enable-basic-metrics";
    public static final boolean BASIC_METRICS_DEFAULT = true;

    /**
     * Whether to share a single set of Metrics objects across all stores. If
     * true, then calls to KeyColumnValueStore methods any store instance in the
     * database will share a common set of Metrics Counters, Timers, Histograms,
     * etc. The prefix for these common metrics will be
     * {@link Backend#METRICS_PREFIX} + {@link Backend#MERGED_METRICS}. If
     * false, then each store has its own set of distinct metrics with a unique
     * name prefix.
     * <p/>
     * This option has no effect when {@link #BASIC_METRICS} is false.
     */
    public static final String MERGE_BASIC_METRICS = "merge-basic-metrics";
    public static final boolean MERGE_BASIC_METRICS_DEFAULT = true;

    /**
     * Metrics console reporter interval in milliseconds. Leaving this
     * configuration key absent or null disables the console reporter.
     */
    public static final String METRICS_CONSOLE_INTERVAL = "console.interval";
    public static final Long METRICS_CONSOLE_INTERVAL_DEFAULT = null;

    /**
     * Metrics CSV reporter interval in milliseconds. Leaving this configuration
     * key absent or null disables the CSV reporter.
     */
    public static final String METRICS_CSV_INTERVAL = "csv.interval";
    public static final Long METRICS_CSV_INTERVAL_DEFAULT = null;
    /**
     * Metrics CSV output directory. It will be created if it doesn't already
     * exist. This option must be non-null if {@link #METRICS_CSV_INTERVAL} is
     * non-null. This option has no effect if {@code #METRICS_CSV_INTERVAL} is
     * null.
     */
    public static final String METRICS_CSV_DIR = "csv.dir";
    public static final String METRICS_CSV_DIR_DEFAULT = null;

    /**
     * Whether to report Metrics through a JMX MBean.
     */
    public static final String METRICS_JMX_ENABLED = "jmx.enabled";
    public static final boolean METRICS_JMX_ENABLED_DEFAULT = false;
    /**
     * The JMX domain in which to report Metrics. If null, then Metrics applies
     * its default value.
     */
    public static final String METRICS_JMX_DOMAIN = "jmx.domain";
    public static final String METRICS_JMX_DOMAIN_DEFAULT = null;
    /**
     * The JMX agentId through which to report Metrics. Calling
     * {@link MBeanServerFactory#findMBeanServer(String)} on this value must
     * return exactly one {@code MBeanServer} at runtime. If null, then Metrics
     * applies its default value.
     */
    public static final String METRICS_JMX_AGENTID = "jmx.agentid";
    public static final String METRICS_JMX_AGENTID_DEFAULT = null;

    /**
     * Metrics Slf4j reporter interval in milliseconds. Leaving this
     * configuration key absent or null disables the Slf4j reporter.
     */
    public static final String METRICS_SLF4J_INTERVAL = "slf4j.interval";
    public static final Long METRICS_SLF4J_INTERVAL_DEFAULT = null;
    /**
     * The complete name of the Logger through which Metrics will report via
     * Slf4j. If non-null, then Metrics will be dumped on
     * {@link LoggerFactory#getLogger(String)} with the configured value as the
     * argument. If null, then Metrics will use its default Slf4j logger.
     */
    public static final String METRICS_SLF4J_LOGGER = "slf4j.logger";
    public static final String METRICS_SLF4J_LOGGER_DEFAULT = null;
    
    /**
     * The configuration namespace within {@link #METRICS_NAMESPACE} for
     * Ganglia.
     */
    public static final String GANGLIA_NAMESPACE = "ganglia";
    
    /**
     * The unicast host or multicast group name to which Metrics will send
     * Ganglia data. Setting this config key has no effect unless
     * {@link #GANGLIA_INTERVAL} is also set.
     */
    public static final String GANGLIA_HOST_OR_GROUP = "hostname";
    
    /**
     * The number of milliseconds to wait between sending Metrics data to the
     * host or group specified by {@link #GANGLIA_HOST_OR_GROUP}. This has no
     * effect unless {@link #GANGLIA_HOST_OR_GROUP} is also set.
     */
    public static final String GANGLIA_INTERVAL = "interval";
    
    /**
     * The port to which Ganglia data are sent.
     * <p>
     * Default = {@value #GANGLIA_PORT_DEFAULT}
     */
    public static final String GANGLIA_PORT = "port";
    public static final int GANGLIA_PORT_DEFAULT = 8649;
    
    /**
     * Whether to interpret {@link #GANGLIA_HOST_OR_GROUP} as a unicast or
     * multicast address. If present, it must be either the string "multicast"
     * or the string "unicast".
     * <p>
     * Default = {@value #GANGLIA_ADDRESSING_MODE_DEFAULT}
     */
    public static final String GANGLIA_ADDRESSING_MODE = "addressing-mode";
    public static final String GANGLIA_ADDRESSING_MODE_DEFAULT = "unicast";
    
    /**
     * The multicast TTL to set on outgoing Ganglia datagrams. This has no
     * effect when {@link #GANGLIA_ADDRESSING_MODE} is "multicast".
     * <p>
     * Default = {@value #GANGLIA_TTL_DEFAULT}
     */
    public static final String GANGLIA_TTL = "ttl";
    public static final int GANGLIA_TTL_DEFAULT = 1;
    
    /**
     * Whether to send data to Ganglia in the 3.1 protocol format (true) or the
     * 3.0 protocol format (false).
     * <p>
     * Default = {@value #GANGLIA_USE_PROTOCOL_31_DEFAULT}
     */
    public static final String GANGLIA_USE_PROTOCOL_31 = "protocol-31";
    public static final boolean GANGLIA_USE_PROTOCOL_31_DEFAULT = true;
    
    /**
     * The host UUID to set on outgoing Ganglia datagrams. If null, no UUID is
     * set on outgoing data.
     * <p>
     * See https://github.com/ganglia/monitor-core/wiki/UUIDSources
     * <p>
     * Default = {@value #GANGLIA_UUID_DEFAULT}
     */
    public static final String GANGLIA_UUID = "uuid";
    public static final UUID GANGLIA_UUID_DEFAULT = null;
    
    /**
     * If non-null, it must be a valid Gmetric spoof string formatted as an
     * IP:hostname pair. If null, Ganglia will automatically determine the IP
     * and hostname to set on outgoing datagrams.
     * <p>
     * See http://sourceforge.net/apps/trac/ganglia/wiki/gmetric_spoofing
     * <p>
     * Default = {@value #GANGLIA_SPOOF_DEFAULT}
     */
    public static final String GANGLIA_SPOOF = "spoof";
    public static final String GANGLIA_SPOOF_DEFAULT = null;
    
    /**
     * The configuration namespace within {@link #METRICS_NAMESPACE} for
     * Graphite.
     */
    public static final String GRAPHITE_NAMESPACE = "graphite";

    /**
     * The hostname to receive Graphite plaintext protocol metric data. Setting
     * this config key has no effect unless {@link #GRAPHITE_INTERVAL} is also
     * set.
     */
    public static final String GRAPHITE_HOST = "hostname";
    
    /**
     * The number of milliseconds to wait between sending Metrics data to the
     * host specified {@link #GRAPHITE_HOST}. This has no effect unless
     * {@link #GRAPHITE_HOST} is also set.
     */
    public static final String GRAPHITE_INTERVAL = "interval";
    
    /**
     * The port to which Graphite data are sent.
     * <p>
     * Default = {@value #GRAPHITE_PORT_DEFAULT}
     */
    public static final String GRAPHITE_PORT = "port";
    public static final int GRAPHITE_PORT_DEFAULT = 2003;
    
    /**
     * A Graphite-specific prefix for reported metrics. If non-null, Metrics
     * prepends this and a "." to all metric names before reporting them to
     * Graphite.
     * <p>
     * Default = {@value #GRAPHITE_PREFIX_DEFAULT}
     */
    public static final String GRAPHITE_PREFIX = "prefix";
    public static final String GRAPHITE_PREFIX_DEFAULT = null;

    private final Configuration configuration;

    private boolean readOnly;
    private boolean flushIDs;
    private boolean batchLoading;
    private long txCacheSize;
    private DefaultTypeMaker defaultTypeMaker;

    public GraphDatabaseConfiguration(String dirOrFile) {
        this(new File(dirOrFile));
    }

    public GraphDatabaseConfiguration(File dirOrFile) {
        this(getConfiguration(dirOrFile));
    }

    public GraphDatabaseConfiguration(Configuration config) {
        Preconditions.checkNotNull(config);
        this.configuration = config;
        preLoadConfiguration();
    }

    /**
     * Load a properties file containing a Titan graph configuration or create a
     * stub configuration for a directory.
     * <p>
     * If the argument is a file:
     * 
     * <ol>
     * <li>Load its contents into a {@link PropertiesConfiguration}</li>
     * <li>For each key starting with {@link #STORAGE_NAMESPACE} and ending in
     * {@link #STORAGE_DIRECTORY_KEY}, check whether the associated value is a
     * non-null, non-absolute path. If so, then prepend the absolute path of the
     * parent directory of {@code dirorFile}. This has the effect of making
     * non-absolute backend paths relative to the config file's directory rather
     * than the JVM's working directory.
     * <li>Return the {@code PropertiesConfiguration}</li>
     * </ol>
     * 
     * <p>
     * Otherwise (if the argument is not a file):
     * <ol>
     * <li>Create a new {@link BaseConfiguration}</li>
     * <li>Set the key STORAGE_DIRECTORY_KEY in namespace STORAGE_NAMESPACE to
     * the absolute path of the argument</li>
     * <li>Return the {@code BaseConfiguration}</li>
     * 
     * @param dirOrFile
     *            A properties file to load or directory in which to read and
     *            write data
     * @return A configuration derived from {@code dirOrFile}
     */
    @SuppressWarnings("unchecked")
    public static final Configuration getConfiguration(File dirOrFile) {
        Preconditions.checkNotNull(dirOrFile, "Need to specify a configuration file or storage directory");

        Configuration configuration;
        
        try {
            if (dirOrFile.isFile()) {
                configuration = new PropertiesConfiguration(dirOrFile);
                
                final File configFileParent = dirOrFile.getParentFile();
                
                Preconditions.checkNotNull(configFileParent);
                Preconditions.checkArgument(configFileParent.isDirectory());
                    
                final Pattern p = Pattern.compile(
                        Pattern.quote(STORAGE_NAMESPACE) + "\\..*" +
                        Pattern.quote(STORAGE_DIRECTORY_KEY));
                
                final Iterator<String> sdKeys = Iterators.filter(configuration.getKeys(), new Predicate<String>() {
                    @Override
                    public boolean apply(String key) {
                        if (null == key)
                            return false;
                        return p.matcher(key).matches();
                    }
                });
                
                while (sdKeys.hasNext()) {
                    String k = sdKeys.next();
                    Preconditions.checkNotNull(k);
                    String s = configuration.getString(k);
                    
                    if (null == s) {
                        log.warn("Configuration key {} has null value", k);
                        continue;
                    }
                    
                    File storedir = new File(s);
                    if (!storedir.isAbsolute()) {
                        configuration.setProperty(k, configFileParent.getAbsolutePath() + File.separator + s);
                        log.debug("Overwrote relative path for key {}: was {}, now {}", k, s, configuration.getProperty(k));
                    } else {
                        log.debug("Loaded absolute path for key {}: {}", k, s);
                    }
                }
            } else {
                configuration = new BaseConfiguration();
                configuration.setProperty(keyInNamespace(STORAGE_NAMESPACE, STORAGE_DIRECTORY_KEY), dirOrFile.getAbsolutePath());
            }
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Could not load configuration at: " + dirOrFile, e);
        }

        return configuration;
    }   

    public static final String toString(Configuration config) {
        StringBuilder s = new StringBuilder();
        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            s.append(key).append(": ").append(config.getProperty(key)).append("\n");
        }
        return s.toString();
    }

    private void preLoadConfiguration() {
        Configuration storageConfig = configuration.subset(STORAGE_NAMESPACE);
        readOnly = storageConfig.getBoolean(STORAGE_READONLY_KEY, STORAGE_READONLY_DEFAULT);
        flushIDs = configuration.subset(IDS_NAMESPACE).getBoolean(IDS_FLUSH_KEY, IDS_FLUSH_DEFAULT);
        batchLoading = storageConfig.getBoolean(STORAGE_BATCH_KEY, STORAGE_BATCH_DEFAULT);
        txCacheSize = configuration.getLong(TX_CACHE_SIZE_KEY, TX_CACHE_SIZE_DEFAULT);
        defaultTypeMaker = preregisteredAutoType.get(configuration.getString(AUTO_TYPE_KEY, AUTO_TYPE_DEFAULT));
        Preconditions.checkNotNull(defaultTypeMaker, "Invalid " + AUTO_TYPE_KEY + " option: " + configuration.getString(AUTO_TYPE_KEY, AUTO_TYPE_DEFAULT));

        //Disable auto-type making when batch-loading is enabled since that may overwrite types without warning
        if (batchLoading) defaultTypeMaker = DisableDefaultTypeMaker.INSTANCE;

        configureMetrics();
    }

    private void configureMetrics() {
        Preconditions.checkNotNull(configuration);

        Configuration metricsConf = configuration.subset(METRICS_NAMESPACE);

        if (null != metricsConf && !metricsConf.isEmpty()) {
            configureMetricsConsoleReporter(metricsConf);
            configureMetricsCsvReporter(metricsConf);
            configureMetricsJmxReporter(metricsConf);
            configureMetricsSlf4jReporter(metricsConf);
            configureMetricsGangliaReporter(metricsConf);
            configureMetricsGraphiteReporter(metricsConf);
        }
    }

    private void configureMetricsConsoleReporter(Configuration conf) {
        Long ms = conf.getLong(METRICS_CONSOLE_INTERVAL, METRICS_CONSOLE_INTERVAL_DEFAULT);
        if (null != ms) {
            MetricManager.INSTANCE.addConsoleReporter(ms);
        }
    }

    private void configureMetricsCsvReporter(Configuration conf) {
        Long ms = conf.getLong(METRICS_CSV_INTERVAL, METRICS_CONSOLE_INTERVAL_DEFAULT);
        String out = conf.getString(METRICS_CSV_DIR, METRICS_CSV_DIR_DEFAULT);
        if (null != ms && null != out) {
            MetricManager.INSTANCE.addCsvReporter(ms, out);
        }
    }

    private void configureMetricsJmxReporter(Configuration conf) {
        boolean enabled = conf.getBoolean(METRICS_JMX_ENABLED, METRICS_JMX_ENABLED_DEFAULT);
        String domain = conf.getString(METRICS_JMX_DOMAIN, METRICS_JMX_DOMAIN_DEFAULT);
        String agentId = conf.getString(METRICS_JMX_AGENTID, METRICS_JMX_AGENTID_DEFAULT);

        if (enabled) {
            MetricManager.INSTANCE.addJmxReporter(domain, agentId);
        }
    }

    private void configureMetricsSlf4jReporter(Configuration conf) {
        Long ms = conf.getLong(METRICS_SLF4J_INTERVAL, METRICS_SLF4J_INTERVAL_DEFAULT);
        // null loggerName is allowed -- that means Metrics will use its internal default
        String loggerName = conf.getString(METRICS_SLF4J_LOGGER, METRICS_SLF4J_LOGGER_DEFAULT);
        if (null != ms) {
            MetricManager.INSTANCE.addSlf4jReporter(ms, loggerName);
        }
    }
    
    private void configureMetricsGangliaReporter(Configuration conf) {
        
        Configuration ganglia = conf.subset(GANGLIA_NAMESPACE);
        
        if (null == ganglia)
            return;
        
        final String host = ganglia.getString(GANGLIA_HOST_OR_GROUP, null);
        final Long ms = ganglia.getLong(GANGLIA_INTERVAL, null);
        
        if (null == host || null == ms) {
            return;
        }

        final Integer port = ganglia.getInt(GANGLIA_PORT, GANGLIA_PORT_DEFAULT);

        final UDPAddressingMode addrMode;
        final String addrModeStr = ganglia.getString(GANGLIA_ADDRESSING_MODE, GANGLIA_ADDRESSING_MODE_DEFAULT);
        if (addrModeStr.toLowerCase().equals("multicast")) {
            addrMode = UDPAddressingMode.MULTICAST;
        } else if (addrModeStr.toLowerCase().equals("unicast")) {
            addrMode = UDPAddressingMode.UNICAST;
        } else {
            throw new RuntimeException("Invalid setting " + METRICS_NAMESPACE
                    + "." + GANGLIA_NAMESPACE + "." + GANGLIA_ADDRESSING_MODE
                    + "=\"" + addrModeStr
                    + "\": must be \"unicast\" or \"multicast\"");
        }
        
        final Boolean proto31 = ganglia.getBoolean(GANGLIA_USE_PROTOCOL_31, GANGLIA_USE_PROTOCOL_31_DEFAULT);

        final int ttl = ganglia.getInt(GANGLIA_TTL, GANGLIA_TTL_DEFAULT);
        
        final UUID uuid;
        final String uuidStr = ganglia.getString(GANGLIA_UUID);
        if (null != uuidStr) {
            uuid = UUID.fromString(uuidStr);
        } else {
            uuid = GANGLIA_UUID_DEFAULT;
        }
        
        String spoof = ganglia.getString(GANGLIA_SPOOF, GANGLIA_SPOOF_DEFAULT);
        if (null != spoof && 0 > spoof.indexOf(':')) {
            throw new RuntimeException("Invalid setting " + METRICS_NAMESPACE
                    + "." + GANGLIA_NAMESPACE + "." + GANGLIA_SPOOF + "=\""
                    + spoof + "\": must be formatted as \"IP:hostname\"");
        }
        
        try {
            MetricManager.INSTANCE.addGangliaReporter(host, port, addrMode, ttl, proto31, uuid, spoof, ms);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void configureMetricsGraphiteReporter(Configuration conf) {
        Configuration graphite = conf.subset(GRAPHITE_NAMESPACE);
        
        if (null == graphite)
            return;
        
        final String host = graphite.getString(GRAPHITE_HOST);
        final Long ms = graphite.getLong(GRAPHITE_INTERVAL);
        
        if (null == host || null == ms) {
            return;
        }

        final Integer port = graphite.getInt(GRAPHITE_PORT, GRAPHITE_PORT_DEFAULT);
        final String prefix = graphite.getString(GRAPHITE_PREFIX, GRAPHITE_PREFIX_DEFAULT);
        
        MetricManager.INSTANCE.addGraphiteReporter(host, port, prefix, ms);
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean hasFlushIDs() {
        return flushIDs;
    }

    public long getTxCacheSize() {
        return txCacheSize;
    }

    public boolean isBatchLoading() {
        return batchLoading;
    }

    public DefaultTypeMaker getDefaultTypeMaker() {
        return defaultTypeMaker;
    }

    public int getWriteAttempts() {
        int attempts = configuration.subset(STORAGE_NAMESPACE).getInt(WRITE_ATTEMPTS_KEY, WRITE_ATTEMPTS_DEFAULT);
        Preconditions.checkArgument(attempts > 0, "Write attempts must be positive");
        return attempts;
    }

    public int getReadAttempts() {
        int attempts = configuration.subset(STORAGE_NAMESPACE).getInt(READ_ATTEMPTS_KEY, READ_ATTEMPTS_DEFAULT);
        Preconditions.checkArgument(attempts > 0, "Read attempts must be positive");
        return attempts;
    }

    public int getStorageWaittime() {
        int time = configuration.subset(STORAGE_NAMESPACE).getInt(STORAGE_ATTEMPT_WAITTIME_KEY, STORAGE_ATTEMPT_WAITTIME_DEFAULT);
        Preconditions.checkArgument(time > 0, "Persistence attempt retry wait time must be positive");
        return time;
    }


    public static List<RegisteredAttributeClass<?>> getRegisteredAttributeClasses(Configuration config) {
        List<RegisteredAttributeClass<?>> all = new ArrayList<RegisteredAttributeClass<?>>();
        Iterator<String> iter = config.getKeys();
        while (iter.hasNext()) {
            String key = iter.next();
            if (!key.startsWith(ATTRIBUTE_PREFIX)) continue;
            try {
                int position = Integer.parseInt(key.substring(ATTRIBUTE_PREFIX.length()));
                Class<?> clazz = null;
                AttributeHandler<?> serializer = null;
                String classname = config.getString(key);
                try {
                    clazz = Class.forName(classname);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Could not find attribute class" + classname);
                }
                Preconditions.checkNotNull(clazz);

                if (config.containsKey(SERIALIZER_PREFIX + position)) {
                    String serializername = config.getString(SERIALIZER_PREFIX + position);
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
                }
                RegisteredAttributeClass reg = new RegisteredAttributeClass(clazz, serializer, position);
                for (int i = 0; i < all.size(); i++) {
                    if (all.get(i).equals(reg)) {
                        throw new IllegalArgumentException("Duplicate attribute registration: " + all.get(i) + " and " + reg);
                    }
                }
                all.add(reg);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid attribute definition: " + key, e);
            }
        }
        Collections.sort(all);
        return all;
    }

    public VertexIDAssigner getIDAssigner(Backend backend) {
        return new VertexIDAssigner(configuration.subset(IDS_NAMESPACE), backend.getIDAuthority(), backend.getStoreFeatures());
    }

    public String getBackendDescription() {
        Configuration storageconfig = configuration.subset(STORAGE_NAMESPACE);
        String clazzname = storageconfig.getString(STORAGE_BACKEND_KEY, STORAGE_BACKEND_DEFAULT);
        if (storageconfig.containsKey(HOSTNAME_KEY)) {
            return clazzname + ":" + storageconfig.getString(HOSTNAME_KEY);
        } else {
            return clazzname + ":" + storageconfig.getString(STORAGE_DIRECTORY_KEY);
        }
    }

    public Backend getBackend() {
        Configuration storageconfig = configuration.subset(STORAGE_NAMESPACE);
        Backend backend = new Backend(storageconfig);
        backend.initialize(storageconfig);
        return backend;
    }

    public Serializer getSerializer() {
        Configuration config = configuration.subset(ATTRIBUTE_NAMESPACE);
        Serializer serializer = new KryoSerializer(config.getBoolean(ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_KEY, ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_DEFAULT));
        for (RegisteredAttributeClass<?> clazz : getRegisteredAttributeClasses(config)) {
            clazz.registerWith(serializer);
        }
        return serializer;
    }

    public boolean hasSerializeAll() {
        return configuration.subset(ATTRIBUTE_NAMESPACE).getBoolean(ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_KEY, ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_DEFAULT);
    }

    public static final String keyInNamespace(String namespace, String key) {
        return namespace + "." + key;
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
        if (!configuration.containsKey(keyInNamespace(STORAGE_NAMESPACE, STORAGE_DIRECTORY_KEY)))
            throw new UnsupportedOperationException("No home directory specified");
        File dir = new File(configuration.getString(keyInNamespace(STORAGE_NAMESPACE, STORAGE_DIRECTORY_KEY)));
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

    static PropertiesConfiguration getPropertiesConfig(String file) {
        PropertiesConfiguration config = new PropertiesConfiguration();
        if (existsFile(file)) {
            try {
                config.load(file);
            } catch (ConfigurationException e) {
                throw new IllegalArgumentException("Cannot load existing configuration file", e);
            }
        }
        config.setFileName(file);
        config.setAutoSave(true);
        return config;
    }


    private static final char CONFIGURATION_SEPARATOR = '.';

    public static Set<String> getUnqiuePrefixes(Configuration config) {
        Set<String> names = new HashSet<String>();
        Iterator<String> keyiter = config.getKeys();
        while (keyiter.hasNext()) {
            String key = keyiter.next();
            int pos = key.indexOf(CONFIGURATION_SEPARATOR);
            if (pos > 0) names.add(key.substring(0, pos));
        }
        return names;
    }
}
