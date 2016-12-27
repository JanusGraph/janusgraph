package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.TitanConfigurationException;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanFactory;

import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.CacheTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.ExpirationKCVSCache;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVSCache;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.NoKCVSCache;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.StandardScanner;
import com.thinkaurelius.titan.diskstorage.locking.Locker;
import com.thinkaurelius.titan.diskstorage.locking.LockerProvider;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStoreManager;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.LogManager;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLog;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore;
import com.thinkaurelius.titan.diskstorage.configuration.backend.KCVSConfiguration;
import com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStoreManager;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Orchestrates and configures all backend systems:
 * The primary backend storage ({@link KeyColumnValueStore}) and all external indexing providers ({@link IndexProvider}).
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Backend implements LockerProvider, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Backend.class);

    /**
     * These are the names for the edge store and property index databases, respectively.
     * The edge store contains all edges and properties. The property index contains an
     * inverted index from attribute value to vertex.
     * <p/>
     * These names are fixed and should NEVER be changed. Changing these strings can
     * disrupt storage adapters that rely on these names for specific configurations.
     */
    public static final String EDGESTORE_NAME = "edgestore";
    public static final String INDEXSTORE_NAME = "graphindex";

    public static final String ID_STORE_NAME = "titan_ids";

    public static final String METRICS_STOREMANAGER_NAME = "storeManager";
    public static final String METRICS_MERGED_STORE = "stores";
    public static final String METRICS_MERGED_CACHE = "caches";
    public static final String METRICS_CACHE_SUFFIX = ".cache";
    public static final String LOCK_STORE_SUFFIX = "_lock_";

    public static final String SYSTEM_TX_LOG_NAME = "txlog";
    public static final String SYSTEM_MGMT_LOG_NAME = "systemlog";

    public static final double EDGESTORE_CACHE_PERCENT = 0.8;
    public static final double INDEXSTORE_CACHE_PERCENT = 0.2;

    private static final long ETERNAL_CACHE_EXPIRATION = 1000l*3600*24*365*200; //200 years

    public static final int THREAD_POOL_SIZE_SCALE_FACTOR = 2;

    public static final Map<String, Integer> STATIC_KEY_LENGTHS = new HashMap<String, Integer>() {{
        put(EDGESTORE_NAME, 8);
        put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, 8);
        put(ID_STORE_NAME, 8);
    }};

    private final KeyColumnValueStoreManager storeManager;
    private final KeyColumnValueStoreManager storeManagerLocking;
    private final StoreFeatures storeFeatures;

    private KCVSCache edgeStore;
    private KCVSCache indexStore;
    private KCVSCache txLogStore;
    private IDAuthority idAuthority;
    private KCVSConfiguration systemConfig;
    private KCVSConfiguration userConfig;
    private boolean hasAttemptedClose;

    private final StandardScanner scanner;

    private final KCVSLogManager mgmtLogManager;
    private final KCVSLogManager txLogManager;
    private final LogManager userLogManager;


    private final Map<String, IndexProvider> indexes;

    private final int bufferSize;
    private final Duration maxWriteTime;
    private final Duration maxReadTime;
    private final boolean cacheEnabled;
    private final ExecutorService threadPool;

    private final Function<String, Locker> lockerCreator;
    private final ConcurrentHashMap<String, Locker> lockers =
            new ConcurrentHashMap<String, Locker>();

    private final Configuration configuration;

    public Backend(Configuration configuration) {
        this.configuration = configuration;

        KeyColumnValueStoreManager manager = getStorageManager(configuration);
        if (configuration.get(BASIC_METRICS)) {
            storeManager = new MetricInstrumentedStoreManager(manager,METRICS_STOREMANAGER_NAME,configuration.get(METRICS_MERGE_STORES),METRICS_MERGED_STORE);
        } else {
            storeManager = manager;
        }
        indexes = getIndexes(configuration);
        storeFeatures = storeManager.getFeatures();

        mgmtLogManager = getKCVSLogManager(MANAGEMENT_LOG);
        txLogManager = getKCVSLogManager(TRANSACTION_LOG);
        userLogManager = getLogManager(USER_LOG);


        cacheEnabled = !configuration.get(STORAGE_BATCH) && configuration.get(DB_CACHE);

        int bufferSizeTmp = configuration.get(BUFFER_SIZE);
        Preconditions.checkArgument(bufferSizeTmp > 0, "Buffer size must be positive");
        if (!storeFeatures.hasBatchMutation()) {
            bufferSize = Integer.MAX_VALUE;
        } else bufferSize = bufferSizeTmp;

        maxWriteTime = configuration.get(STORAGE_WRITE_WAITTIME);
        maxReadTime = configuration.get(STORAGE_READ_WAITTIME);

        if (!storeFeatures.hasLocking()) {
            Preconditions.checkArgument(storeFeatures.isKeyConsistent(),"Store needs to support some form of locking");
            storeManagerLocking = new ExpectedValueCheckingStoreManager(storeManager,LOCK_STORE_SUFFIX,this,maxReadTime);
        } else {
            storeManagerLocking = storeManager;
        }

        if (configuration.get(PARALLEL_BACKEND_OPS)) {
            int poolsize = Runtime.getRuntime().availableProcessors() * THREAD_POOL_SIZE_SCALE_FACTOR;
            threadPool = Executors.newFixedThreadPool(poolsize);
            log.info("Initiated backend operations thread pool of size {}", poolsize);
        } else {
            threadPool = null;
        }

        final String lockBackendName = configuration.get(LOCK_BACKEND);
        if (REGISTERED_LOCKERS.containsKey(lockBackendName)) {
            lockerCreator = REGISTERED_LOCKERS.get(lockBackendName);
        } else {
            throw new TitanConfigurationException("Unknown lock backend \"" +
                    lockBackendName + "\".  Known lock backends: " +
                    Joiner.on(", ").join(REGISTERED_LOCKERS.keySet()) + ".");
        }
        // Never used for backends that have innate transaction support, but we
        // want to maintain the non-null invariant regardless; it will default
        // to connsistentkey impl if none is specified
        Preconditions.checkNotNull(lockerCreator);

        scanner = new StandardScanner(storeManager);
    }


    @Override
    public Locker getLocker(String lockerName) {

        Preconditions.checkNotNull(lockerName);

        Locker l = lockers.get(lockerName);

        if (null == l) {
            l = lockerCreator.apply(lockerName);
            final Locker x = lockers.putIfAbsent(lockerName, l);
            if (null != x) {
                l = x;
            }
        }

        return l;
    }


    /**
     * Initializes this backend with the given configuration. Must be called before this Backend can be used
     *
     * @param config
     */
    public void initialize(Configuration config) {
        try {
            //EdgeStore & VertexIndexStore
            KeyColumnValueStore idStore = storeManager.openDatabase(ID_STORE_NAME);

            idAuthority = null;
            if (storeFeatures.isKeyConsistent()) {
                idAuthority = new ConsistentKeyIDAuthority(idStore, storeManager, config);
            } else {
                throw new IllegalStateException("Store needs to support consistent key or transactional operations for ID manager to guarantee proper id allocations");
            }

            KeyColumnValueStore edgeStoreRaw = storeManagerLocking.openDatabase(EDGESTORE_NAME);
            KeyColumnValueStore indexStoreRaw = storeManagerLocking.openDatabase(INDEXSTORE_NAME);

            //Configure caches
            if (cacheEnabled) {
                long expirationTime = configuration.get(DB_CACHE_TIME);
                Preconditions.checkArgument(expirationTime>=0,"Invalid cache expiration time: %s",expirationTime);
                if (expirationTime==0) expirationTime=ETERNAL_CACHE_EXPIRATION;

                long cacheSizeBytes;
                double cachesize = configuration.get(DB_CACHE_SIZE);
                Preconditions.checkArgument(cachesize>0.0,"Invalid cache size specified: %s",cachesize);
                if (cachesize<1.0) {
                    //Its a percentage
                    Runtime runtime = Runtime.getRuntime();
                    cacheSizeBytes = (long)((runtime.maxMemory()-(runtime.totalMemory()-runtime.freeMemory())) * cachesize);
                } else {
                    Preconditions.checkArgument(cachesize>1000,"Cache size is too small: %s",cachesize);
                    cacheSizeBytes = (long)cachesize;
                }
                log.info("Configuring total store cache size: {}",cacheSizeBytes);
                long cleanWaitTime = configuration.get(DB_CACHE_CLEAN_WAIT);
                Preconditions.checkArgument(EDGESTORE_CACHE_PERCENT + INDEXSTORE_CACHE_PERCENT == 1.0,"Cache percentages don't add up!");
                long edgeStoreCacheSize = Math.round(cacheSizeBytes * EDGESTORE_CACHE_PERCENT);
                long indexStoreCacheSize = Math.round(cacheSizeBytes * INDEXSTORE_CACHE_PERCENT);

                edgeStore = new ExpirationKCVSCache(edgeStoreRaw,getMetricsCacheName(EDGESTORE_NAME),expirationTime,cleanWaitTime,edgeStoreCacheSize);
                indexStore = new ExpirationKCVSCache(indexStoreRaw,getMetricsCacheName(INDEXSTORE_NAME),expirationTime,cleanWaitTime,indexStoreCacheSize);
            } else {
                edgeStore = new NoKCVSCache(edgeStoreRaw);
                indexStore = new NoKCVSCache(indexStoreRaw);
            }

            //Just open them so that they are cached
            txLogManager.openLog(SYSTEM_TX_LOG_NAME);
            mgmtLogManager.openLog(SYSTEM_MGMT_LOG_NAME);
            txLogStore = new NoKCVSCache(storeManager.openDatabase(SYSTEM_TX_LOG_NAME));


            //Open global configuration
            KeyColumnValueStore systemConfigStore = storeManagerLocking.openDatabase(SYSTEM_PROPERTIES_STORE_NAME);
            systemConfig = getGlobalConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return storeManagerLocking.beginTransaction(StandardBaseTransactionConfig.of(
                            configuration.get(TIMESTAMP_PROVIDER),
                            storeFeatures.getKeyConsistentTxConfig()));
                }

                @Override
                public void close() throws BackendException {
                    //Do nothing, storeManager is closed explicitly by Backend
                }
            },systemConfigStore,configuration);
            userConfig = getConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return storeManagerLocking.beginTransaction(StandardBaseTransactionConfig.of(configuration.get(TIMESTAMP_PROVIDER)));
                }

                @Override
                public void close() throws BackendException {
                    //Do nothing, storeManager is closed explicitly by Backend
                }
            },systemConfigStore,USER_CONFIGURATION_IDENTIFIER,configuration);

        } catch (BackendException e) {
            throw new TitanException("Could not initialize backend", e);
        }
    }

    /**
     * Get information about all registered {@link IndexProvider}s.
     *
     * @return
     */
    public Map<String, IndexInformation> getIndexInformation() {
        ImmutableMap.Builder<String, IndexInformation> copy = ImmutableMap.builder();
        copy.putAll(indexes);
        return copy.build();
    }
//
//    public IndexProvider getIndexProvider(String name) {
//        return indexes.get(name);
//    }

    public KCVSLog getSystemTxLog() {
        try {
            return txLogManager.openLog(SYSTEM_TX_LOG_NAME);
        } catch (BackendException e) {
            throw new TitanException("Could not re-open transaction log", e);
        }
    }

    public Log getSystemMgmtLog() {
        try {
            return mgmtLogManager.openLog(SYSTEM_MGMT_LOG_NAME);
        } catch (BackendException e) {
            throw new TitanException("Could not re-open management log", e);
        }
    }

    public StandardScanner.Builder buildEdgeScanJob() {
        return buildStoreIndexScanJob(EDGESTORE_NAME);
    }

    public StandardScanner.Builder buildGraphIndexScanJob() {
        return buildStoreIndexScanJob(INDEXSTORE_NAME);
    }

    private StandardScanner.Builder buildStoreIndexScanJob(String storeName) {
        TimestampProvider provider = configuration.get(TIMESTAMP_PROVIDER);
        ModifiableConfiguration jobConfig = GraphDatabaseConfiguration.buildJobConfiguration();
        jobConfig.set(JOB_START_TIME,provider.getTime().toEpochMilli());
        return scanner.build()
                .setStoreName(storeName)
                .setTimestampProvider(provider)
                .setJobConfiguration(jobConfig)
                .setGraphConfiguration(configuration)
                .setNumProcessingThreads(1)
                .setWorkBlockSize(10000);
    }

    public TitanManagement.IndexJobFuture getScanJobStatus(Object jobId) {
        return scanner.getRunningJob(jobId);
    }

    public Log getUserLog(String identifier) throws BackendException {
        return userLogManager.openLog(getUserLogName(identifier));
    }

    public static final String getUserLogName(String identifier) {
        Preconditions.checkArgument(StringUtils.isNotBlank(identifier));
        return USER_LOG_PREFIX +identifier;
    }

    public KCVSConfiguration getGlobalSystemConfig() {
        return systemConfig;
    }

    public KCVSConfiguration getUserConfiguration() {
        return userConfig;
    }

    private String getMetricsCacheName(String storeName) {
        if (!configuration.get(BASIC_METRICS)) return null;
        return configuration.get(METRICS_MERGE_STORES) ? METRICS_MERGED_CACHE : storeName + METRICS_CACHE_SUFFIX;
    }

    public KCVSLogManager getKCVSLogManager(String logName) {
        Preconditions.checkArgument(configuration.restrictTo(logName).get(LOG_BACKEND).equalsIgnoreCase(LOG_BACKEND.getDefaultValue()));
        return (KCVSLogManager)getLogManager(logName);
    }

    public LogManager getLogManager(String logName) {
        return getLogManager(configuration, logName, storeManager);
    }

    private static LogManager getLogManager(Configuration config, String logName, KeyColumnValueStoreManager sm) {
        Configuration logConfig = config.restrictTo(logName);
        String backend = logConfig.get(LOG_BACKEND);
        if (backend.equalsIgnoreCase(LOG_BACKEND.getDefaultValue())) {
            return new KCVSLogManager(sm,logConfig);
        } else {
            Preconditions.checkArgument(config!=null);
            LogManager lm = getImplementationClass(logConfig,logConfig.get(LOG_BACKEND),REGISTERED_LOG_MANAGERS);
            Preconditions.checkNotNull(lm);
            return lm;
        }

    }

    public static KeyColumnValueStoreManager getStorageManager(Configuration storageConfig) {
        StoreManager manager = getImplementationClass(storageConfig, storageConfig.get(STORAGE_BACKEND),
                StandardStoreManager.getAllManagerClasses());
        if (manager instanceof OrderedKeyValueStoreManager) {
            manager = new OrderedKeyValueStoreManagerAdapter((OrderedKeyValueStoreManager) manager, STATIC_KEY_LENGTHS);
        }
        Preconditions.checkArgument(manager instanceof KeyColumnValueStoreManager,"Invalid storage manager: %s",manager.getClass());
        return (KeyColumnValueStoreManager) manager;
    }

    private static KCVSConfiguration getGlobalConfiguration(final BackendOperation.TransactionalProvider txProvider,
                                                                     final KeyColumnValueStore store,
                                                                     final Configuration config) {
        return getConfiguration(txProvider, store, SYSTEM_CONFIGURATION_IDENTIFIER, config);
    }

    private static KCVSConfiguration getConfiguration(final BackendOperation.TransactionalProvider txProvider,
                                                            final KeyColumnValueStore store, final String identifier,
                                                            final Configuration config) {
        try {
            KCVSConfiguration kcvsConfig =
                    new KCVSConfiguration(txProvider,config,store,identifier);
            kcvsConfig.setMaxOperationWaitTime(config.get(SETUP_WAITTIME));
            return kcvsConfig;
        } catch (BackendException e) {
            throw new TitanException("Could not open global configuration",e);
        }
    }

    public static KCVSConfiguration getStandaloneGlobalConfiguration(final KeyColumnValueStoreManager manager,
                                                                     final Configuration config) {
        try {
            final StoreFeatures features = manager.getFeatures();
            return getGlobalConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return manager.beginTransaction(StandardBaseTransactionConfig.of(config.get(TIMESTAMP_PROVIDER),features.getKeyConsistentTxConfig()));
                }

                @Override
                public void close() throws BackendException {
                    manager.close();
                }
            },manager.openDatabase(SYSTEM_PROPERTIES_STORE_NAME),config);
        } catch (BackendException e) {
            throw new TitanException("Could not open global configuration",e);
        }
    }

    private final static Map<String, IndexProvider> getIndexes(Configuration config) {
        ImmutableMap.Builder<String, IndexProvider> builder = ImmutableMap.builder();
        for (String index : config.getContainedNamespaces(INDEX_NS)) {
            Preconditions.checkArgument(StringUtils.isNotBlank(index), "Invalid index name [%s]", index);
            log.info("Configuring index [{}]", index);
            IndexProvider provider = getImplementationClass(config.restrictTo(index), config.get(INDEX_BACKEND,index),
                    StandardIndexProvider.getAllProviderClasses());
            Preconditions.checkNotNull(provider);
            builder.put(index, provider);
        }
        return builder.build();
    }

    public final static <T> T getImplementationClass(Configuration config, String clazzname, Map<String, String> registeredImpls) {
        if (registeredImpls.containsKey(clazzname.toLowerCase())) {
            clazzname = registeredImpls.get(clazzname.toLowerCase());
        }

        return ConfigurationUtil.instantiate(clazzname, new Object[]{config}, new Class[]{Configuration.class});
    }


    /**
     * Returns the configured {@link IDAuthority}.
     *
     * @return
     */
    public IDAuthority getIDAuthority() {
        Preconditions.checkNotNull(idAuthority, "Backend has not yet been initialized");
        return idAuthority;
    }

    /**
     * Returns the {@link StoreFeatures} of the configured backend storage engine.
     *
     * @return
     */
    public StoreFeatures getStoreFeatures() {
        return storeFeatures;
    }

    public Class<? extends KeyColumnValueStoreManager> getStoreManagerClass() {
        return storeManager.getClass();
    }

    public StoreManager getStoreManager() {
        return storeManager;
    }

    /**
     * Returns the {@link IndexFeatures} of all configured index backends
     */
    public Map<String,IndexFeatures> getIndexFeatures() {
        return Maps.transformValues(indexes,new Function<IndexProvider, IndexFeatures>() {
            @Nullable
            @Override
            public IndexFeatures apply(@Nullable IndexProvider indexProvider) {
                return indexProvider.getFeatures();
            }
        });
    }

    /**
     * Opens a new transaction against all registered backend system wrapped in one {@link BackendTransaction}.
     *
     * @return
     * @throws BackendException
     */
    public BackendTransaction beginTransaction(TransactionConfiguration configuration, KeyInformation.Retriever indexKeyRetriever) throws BackendException {

        StoreTransaction tx = storeManagerLocking.beginTransaction(configuration);

        // Cache
        CacheTransaction cacheTx = new CacheTransaction(tx, storeManagerLocking, bufferSize, maxWriteTime, configuration.hasEnabledBatchLoading());

        // Index transactions
        Map<String, IndexTransaction> indexTx = new HashMap<String, IndexTransaction>(indexes.size());
        for (Map.Entry<String, IndexProvider> entry : indexes.entrySet()) {
            indexTx.put(entry.getKey(), new IndexTransaction(entry.getValue(), indexKeyRetriever.get(entry.getKey()), configuration, maxWriteTime));
        }

        return new BackendTransaction(cacheTx, configuration, storeFeatures,
                edgeStore, indexStore, txLogStore,
                maxReadTime, indexTx, threadPool);
    }

    public synchronized void close() throws BackendException {
        if (!hasAttemptedClose) {
            hasAttemptedClose = true;
            mgmtLogManager.close();
            txLogManager.close();
            userLogManager.close();

            scanner.close();
            edgeStore.close();
            indexStore.close();
            idAuthority.close();
            systemConfig.close();
            userConfig.close();
            storeManager.close();
            if(threadPool != null) {
            	threadPool.shutdown();
            }
            //Indexes
            for (IndexProvider index : indexes.values()) index.close();
        } else {
            log.debug("Backend {} has already been closed or cleared", this);
        }
    }

    /**
     * Clears the storage of all registered backend data providers. This includes backend storage engines and index providers.
     * <p/>
     * IMPORTANT: Clearing storage means that ALL data will be lost and cannot be recovered.
     *
     * @throws BackendException
     */
    public synchronized void clearStorage() throws BackendException {
        if (!hasAttemptedClose) {
            hasAttemptedClose = true;
            mgmtLogManager.close();
            txLogManager.close();
            userLogManager.close();

            scanner.close();
            edgeStore.close();
            indexStore.close();
            idAuthority.close();
            systemConfig.close();
            userConfig.close();
            storeManager.clearStorage();
            storeManager.close();
            //Indexes
            for (IndexProvider index : indexes.values()) {
                index.clearStorage();
                index.close();
            }
        } else {
            log.debug("Backend {} has already been closed or cleared", this);
        }
    }

    //############ Registered Storage Managers ##############

    private static final ImmutableMap<StandardStoreManager, ConfigOption<?>> STORE_SHORTHAND_OPTIONS;

    static {
        Map<StandardStoreManager, ConfigOption<?>> m =
                new HashMap<StandardStoreManager, ConfigOption<?>>();

        m.put(StandardStoreManager.BDB_JE, STORAGE_DIRECTORY);
        m.put(StandardStoreManager.CASSANDRA_ASTYANAX, STORAGE_HOSTS);
        m.put(StandardStoreManager.CASSANDRA_EMBEDDED, STORAGE_CONF_FILE);
        m.put(StandardStoreManager.CASSANDRA_THRIFT, STORAGE_HOSTS);
        m.put(StandardStoreManager.HBASE, STORAGE_HOSTS);
        //m.put(StandardStorageBackend.IN_MEMORY, null);

        //STORE_SHORTHAND_OPTIONS = Maps.immutableEnumMap(m);
        STORE_SHORTHAND_OPTIONS = ImmutableMap.copyOf(m);
    }

    public static ConfigOption<?> getOptionForShorthand(String shorthand) {
        if (null == shorthand)
            return null;

        shorthand = shorthand.toLowerCase();

        for (StandardStoreManager m : STORE_SHORTHAND_OPTIONS.keySet()) {
            if (m.getShorthands().contains(shorthand))
                return STORE_SHORTHAND_OPTIONS.get(m);
        }

        return null;
    }

    public static final Map<String,String> REGISTERED_LOG_MANAGERS = new HashMap<String, String>() {{
        put("default","com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager");
    }};

    private final Function<String, Locker> CONSISTENT_KEY_LOCKER_CREATOR = new Function<String, Locker>() {
        @Override
        public Locker apply(String lockerName) {
            KeyColumnValueStore lockerStore;
            try {
                lockerStore = storeManager.openDatabase(lockerName);
            } catch (BackendException e) {
                throw new TitanConfigurationException("Could not retrieve store named " + lockerName + " for locker configuration", e);
            }
            return new ConsistentKeyLocker.Builder(lockerStore, storeManager).fromConfig(configuration).build();
        }
    };

    private final Function<String, Locker> ASTYANAX_RECIPE_LOCKER_CREATOR = new Function<String, Locker>() {

        @Override
        public Locker apply(String lockerName) {

            String expectedManagerName = "com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager";
            String actualManagerName = storeManager.getClass().getCanonicalName();
            // Require AstyanaxStoreManager
            Preconditions.checkArgument(expectedManagerName.equals(actualManagerName),
                    "Astyanax Recipe locker is only supported with the Astyanax storage backend (configured:"
                            + actualManagerName + " != required:" + expectedManagerName + ")");

            try {
                Class<?> c = storeManager.getClass();
                Method method = c.getMethod("openLocker", String.class);
                Object o = method.invoke(storeManager, lockerName);
                return (Locker) o;
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Could not find method when configuring locking with Astyanax Recipes");
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Could not access method when configuring locking with Astyanax Recipes", e);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException("Could not invoke method when configuring locking with Astyanax Recipes", e);
            }
        }
    };

    private final Function<String, Locker> TEST_LOCKER_CREATOR = new Function<String, Locker>() {

        @Override
        public Locker apply(String lockerName) {
            return openManagedLocker("com.thinkaurelius.titan.diskstorage.util.TestLockerManager",lockerName);

        }
    };

    private final Map<String, Function<String, Locker>> REGISTERED_LOCKERS = ImmutableMap.of(
            "consistentkey", CONSISTENT_KEY_LOCKER_CREATOR,
            "astyanaxrecipe", ASTYANAX_RECIPE_LOCKER_CREATOR,
            "test", TEST_LOCKER_CREATOR
    );

    private static Locker openManagedLocker(String classname, String lockerName) {
        try {
            Class c = Class.forName(classname);
            Constructor constructor = c.getConstructor();
            Object instance = constructor.newInstance();
            Method method = c.getMethod("openLocker", String.class);
            Object o = method.invoke(instance, lockerName);
            return (Locker) o;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find implementation class: " + classname);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Could not instantiate implementation: " + classname, e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Could not find method when configuring locking for: " + classname,e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Could not access method when configuring locking for: " + classname,e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Could not invoke method when configuring locking for: " + classname,e);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate implementation: " + classname, e);
        }
    }
}
