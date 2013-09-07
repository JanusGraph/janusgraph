package com.thinkaurelius.titan.graphdb.configuration;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.graphdb.types.DisableDefaultTypeMaker;
import com.thinkaurelius.titan.util.stats.MetricManager;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerFactory;
import java.io.File;
import java.util.*;

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

    /**
     * Configures the {@link DefaultTypeMaker} to be used by this graph. If left empty, automatic creation of types
     * is disabled.
     */
    public static final String AUTO_TYPE_KEY = "autotype";
    public static final String AUTO_TYPE_DEFAULT = "../../titan-core/src/test/java/com/thinkaurelius/titan/blueprints";

    private static final Map<String, DefaultTypeMaker> preregisteredAutoType = new HashMap<String, DefaultTypeMaker>() {{
        put("none", DisableDefaultTypeMaker.INSTANCE);
        put("../../titan-core/src/test/java/com/thinkaurelius/titan/blueprints", BlueprintsDefaultTypeMaker.INSTANCE);
    }};


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
     * <p>
     * This option has no effect when {@link #BASIC_METRICS} is false.
     */
    public static final String MERGE_BASIC_METRICS = "merge-basic-metrics";
    public static final boolean MERGE_BASIC_METRICS_DEFAULT = true; 

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
     *
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

    // ############## Attributes ######################
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
    
    
    private final Configuration configuration;

    private boolean readOnly;
    private boolean flushIDs;
    private boolean batchLoading;
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

    public static final Configuration getConfiguration(File dirOrFile) {
        Preconditions.checkNotNull(dirOrFile, "Need to specify a configuration file or storage directory");

        Configuration configuration;

        try {
            if (dirOrFile.isFile())
                return new PropertiesConfiguration(dirOrFile);
            
            configuration = new BaseConfiguration();
            configuration.setProperty(keyInNamespace(STORAGE_NAMESPACE, STORAGE_DIRECTORY_KEY), dirOrFile.getAbsolutePath());
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
        defaultTypeMaker = preregisteredAutoType.get(configuration.getString(AUTO_TYPE_KEY, AUTO_TYPE_DEFAULT));
        Preconditions.checkNotNull(defaultTypeMaker, "Invalid " + AUTO_TYPE_KEY + " option: " + configuration.getString(AUTO_TYPE_KEY, AUTO_TYPE_DEFAULT));
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

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean hasFlushIDs() {
        return flushIDs;
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
            if (pos>0) names.add(key.substring(0,pos));
        }
        return names;
    }
}
