package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanConfigurationException;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.Locker;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import com.thinkaurelius.titan.diskstorage.locking.transactional.TransactionalLockStore;
import com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore;
import com.thinkaurelius.titan.diskstorage.configuration.backend.KCVSConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.graphdb.database.indexing.StandardIndexInformation;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Orchestrates and configures all backend systems:
 * The primary backend storage ({@link KeyColumnValueStore}) and all external indexing providers ({@link IndexProvider}).
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Backend {

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
    public static final String VERTEXINDEX_STORE_NAME = "vertexindex";
    public static final String EDGEINDEX_STORE_NAME = "edgeindex";

    public static final String ID_STORE_NAME = "titan_ids";

    public static final String TITAN_BACKEND_VERSION = "titan-version";
    public static final String MERGED_METRICS = "stores";
    public static final String LOCK_STORE_SUFFIX = "_lock_";

    public static final String SYSTEM_PROPERTIES_IDENTIFIER = "general";

    public static final int THREAD_POOL_SIZE_SCALE_FACTOR = 2;

    public static final Map<String, Integer> STATIC_KEY_LENGTHS = new HashMap<String, Integer>() {{
        put(EDGESTORE_NAME, 8);
        put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, 8);
        put(ID_STORE_NAME, 4);
    }};

    private final KeyColumnValueStoreManager storeManager;
    private final StoreFeatures storeFeatures;

    private KeyColumnValueStore edgeStore;
    private KeyColumnValueStore vertexIndexStore;
    private KeyColumnValueStore edgeIndexStore;
    private IDAuthority idAuthority;

    private final Map<String, IndexProvider> indexes;

    private final int bufferSize;
    private final boolean hashPrefixIndex;
    private final boolean basicMetrics;
    private final boolean mergeBasicMetrics;

    private final int writeAttempts;
    private final int readAttempts;
    private final int persistAttemptWaittime;
    private final ExecutorService threadPool;

    private final Function<String, Locker> lockerCreator;
    private final ConcurrentHashMap<String, Locker> lockers =
            new ConcurrentHashMap<String, Locker>();

    private final Configuration configuration;

    public Backend(Configuration configuration) {
        this.configuration = configuration;

        storeManager = getStorageManager(configuration);
        indexes = getIndexes(configuration);
        storeFeatures = storeManager.getFeatures();

        basicMetrics = configuration.get(BASIC_METRICS);
        mergeBasicMetrics = configuration.get(MERGE_BASIC_METRICS);

        int bufferSizeTmp = configuration.get(BUFFER_SIZE);
        Preconditions.checkArgument(bufferSizeTmp >= 0, "Buffer size must be non-negative (use 0 to disable)");
        if (!storeFeatures.supportsBatchMutation()) {
            bufferSize = 0;
            log.debug("Buffering disabled because backend does not support batch mutations");
        } else bufferSize = bufferSizeTmp;

        writeAttempts = configuration.get(WRITE_ATTEMPTS);
        readAttempts = configuration.get(READ_ATTEMPTS);
        persistAttemptWaittime = configuration.get(STORAGE_ATTEMPT_WAITTIME);

        if (configuration.get(PARALLEL_BACKEND_OPS)) {
            int poolsize = Math.min(1, Runtime.getRuntime().availableProcessors()) * THREAD_POOL_SIZE_SCALE_FACTOR;
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

        if (storeFeatures.isDistributed() && storeFeatures.isKeyOrdered()) {
            log.debug("Wrapping index store with HashPrefix");
            hashPrefixIndex = true;
        } else {
            hashPrefixIndex = false;
        }
    }

    public KeyColumnValueStoreManager getStoreManager() {
        return storeManager;
    }

    private KeyColumnValueStore getLockStore(KeyColumnValueStore store) throws StorageException {
        return getLockStore(store, true);
    }

    private KeyColumnValueStore getLockStore(KeyColumnValueStore store, boolean lockEnabled) throws StorageException {
        if (!storeFeatures.supportsLocking()) {
            if (storeFeatures.supportsTxIsolation()) {
                store = new TransactionalLockStore(store);
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                if (lockEnabled) {
                    final String lockerName = store.getName() + LOCK_STORE_SUFFIX;
                    store = new ExpectedValueCheckingStore(store, getLocker(lockerName));
                } else {
                    store = new ExpectedValueCheckingStore(store, null);
                }
            } else throw new IllegalArgumentException("Store needs to support some form of locking");
        }
        return store;
    }

    private Locker getLocker(String lockerName) {

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

    private KeyColumnValueStore getBufferStore(String name) throws StorageException {
        Preconditions.checkArgument(bufferSize <= 1 || storeManager.getFeatures().supportsBatchMutation());
        KeyColumnValueStore store = null;
        store = storeManager.openDatabase(name);
        if (bufferSize > 1) {
            store = new BufferedKeyColumnValueStore(store, true);
        }
        //Enable cache
        store = new CachedKeyColumnValueStore(store);
        return store;
    }

    private KeyColumnValueStore getStore(String name) throws StorageException {
        KeyColumnValueStore store = storeManager.openDatabase(name);
        return store;
    }

    /**
     * Initializes this backend with the given configuration. Must be called before this Backend can be used
     *
     * @param config
     */
    public void initialize(Configuration config) {
        try {
            //EdgeStore & VertexIndexStore
            KeyColumnValueStore idStore = getStore(ID_STORE_NAME);
            if (basicMetrics) {
                idStore = new MetricInstrumentedStore(idStore, getMetricsStoreName("idStore"));
            }
            idAuthority = null;
            if (storeFeatures.supportsConsistentKeyOperations()) {
                idAuthority = new ConsistentKeyIDManager(idStore, storeManager, config);
            } else {
                throw new IllegalStateException("Store needs to support consistent key or transactional operations for ID manager to guarantee proper id allocations");
            }

            edgeStore = getLockStore(getBufferStore(EDGESTORE_NAME));
            vertexIndexStore = getLockStore(getBufferStore(VERTEXINDEX_STORE_NAME));
            edgeIndexStore = getLockStore(getBufferStore(EDGEINDEX_STORE_NAME), false);


            if (hashPrefixIndex) {
                vertexIndexStore = new HashPrefixKeyColumnValueStore(vertexIndexStore, 4);
                edgeIndexStore = new HashPrefixKeyColumnValueStore(edgeIndexStore, 4);
            }

            if (basicMetrics) {
                edgeStore = new MetricInstrumentedStore(edgeStore, getMetricsStoreName("edgeStore"));
                vertexIndexStore = new MetricInstrumentedStore(vertexIndexStore, getMetricsStoreName("vertexIndexStore"));
                edgeIndexStore = new MetricInstrumentedStore(edgeIndexStore, getMetricsStoreName("edgeIndexStore"));
            }

            String version = null;
            KCVSConfiguration systemConfig = new KCVSConfiguration(storeManager,SYSTEM_PROPERTIES_STORE_NAME,
                                                        SYSTEM_PROPERTIES_IDENTIFIER);
            try {
                systemConfig.setMaxOperationWaitTime(config.get(SETUP_WAITTIME));
                version = systemConfig.get(TITAN_BACKEND_VERSION,String.class);
                if (version == null) {
                    systemConfig.set(TITAN_BACKEND_VERSION, TitanConstants.VERSION);
                    version = TitanConstants.VERSION;
                }
            } finally {
                systemConfig.close();
            }
            Preconditions.checkState(version != null, "Could not read version from storage backend");
            if (!TitanConstants.VERSION.equals(version) && !TitanConstants.COMPATIBLE_VERSIONS.contains(version)) {
                throw new TitanException("StorageBackend version is incompatible with current Titan version: " + version + " vs. " + TitanConstants.VERSION);
            }
        } catch (StorageException e) {
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
        copy.put(Titan.Token.STANDARD_INDEX, StandardIndexInformation.INSTANCE);
        return copy.build();
    }

    private String getMetricsStoreName(String storeName) {
        return mergeBasicMetrics ? MERGED_METRICS : storeName;
    }

    public final static KeyColumnValueStoreManager getStorageManager(Configuration storageConfig) {
        StoreManager manager = getImplementationClass(storageConfig, storageConfig.get(STORAGE_BACKEND),
                REGISTERED_STORAGE_MANAGERS);
        if (manager instanceof OrderedKeyValueStoreManager) {
            manager = new OrderedKeyValueStoreManagerAdapter((OrderedKeyValueStoreManager) manager, STATIC_KEY_LENGTHS);
        } else if (manager instanceof CacheStoreManager) {
            manager = new CacheStoreManagerAdapter((CacheStoreManager) manager);
        }
        Preconditions.checkArgument(manager instanceof KeyColumnValueStoreManager);
        return (KeyColumnValueStoreManager) manager;
    }

    private final static Map<String, IndexProvider> getIndexes(Configuration config) {
        ImmutableMap.Builder<String, IndexProvider> builder = ImmutableMap.builder();
        for (String index : config.getContainedNamespaces(INDEX_NS)) {
            Preconditions.checkArgument(StringUtils.isNotBlank(index), "Invalid index name [%s]", index);
            log.info("Configuring index [{}]", index);
            IndexProvider provider = getImplementationClass(config.restrictTo(index), config.get(INDEX_BACKEND,index),
                    REGISTERED_INDEX_PROVIDERS);
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

    //1. Store
//
//    public KeyColumnValueStore getEdgeStore() {
//        Preconditions.checkNotNull(edgeStore, "Backend has not yet been initialized");
//        return edgeStore;
//    }
//
//    public KeyColumnValueStore getVertexIndexStore() {
//        Preconditions.checkNotNull(vertexIndexStore, "Backend has not yet been initialized");
//        return vertexIndexStore;
//    }

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
        return storeManager.getFeatures();
    }

    //3. Messaging queues

    /**
     * Opens a new transaction against all registered backend system wrapped in one {@link BackendTransaction}.
     *
     * @return
     * @throws StorageException
     */
    public BackendTransaction beginTransaction(TransactionConfiguration configuration, KeyInformation.Retriever indexKeyRetriever) throws StorageException {
        StoreTxConfig txConfig = new StoreTxConfig(configuration.getMetricsPrefix());
        if (configuration.hasTimestamp()) txConfig.setTimestamp(configuration.getTimestamp());
        StoreTransaction tx = storeManager.beginTransaction(txConfig);
        if (bufferSize > 1) {
            Preconditions.checkArgument(storeManager.getFeatures().supportsBatchMutation());
            tx = new BufferTransaction(tx, storeManager, bufferSize, writeAttempts, persistAttemptWaittime);
        }
        if (!storeFeatures.supportsLocking()) {
            if (storeFeatures.supportsTxIsolation()) {
                //No transaction wrapping needed
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                txConfig = new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT, configuration.getMetricsPrefix());
                if (configuration.hasTimestamp()) txConfig.setTimestamp(configuration.getTimestamp());
                tx = new ExpectedValueCheckingTransaction(tx,
                        storeManager.beginTransaction(txConfig),
                        readAttempts);
            }
        }

        //Index transactions
        Map<String, IndexTransaction> indexTx = new HashMap<String, IndexTransaction>(indexes.size());
        for (Map.Entry<String, IndexProvider> entry : indexes.entrySet()) {
            indexTx.put(entry.getKey(), new IndexTransaction(entry.getValue(), indexKeyRetriever.get(entry.getKey())));
        }

        return new BackendTransaction(tx, storeManager.getFeatures(),
                edgeStore, vertexIndexStore, edgeIndexStore,
                readAttempts, persistAttemptWaittime, indexTx, threadPool);
    }

    public void close() throws StorageException {
        edgeStore.close();
        vertexIndexStore.close();
        edgeIndexStore.close();
        idAuthority.close();
        storeManager.close();
        threadPool.shutdown();
        //Indexes
        for (IndexProvider index : indexes.values()) index.close();
    }

    /**
     * Clears the storage of all registered backend data providers. This includes backend storage engines and index providers.
     * <p/>
     * IMPORTANT: Clearing storage means that ALL data will be lost and cannot be recovered.
     *
     * @throws StorageException
     */
    public void clearStorage() throws StorageException {
        edgeStore.close();
        vertexIndexStore.close();
        edgeIndexStore.close();
        idAuthority.close();
        storeManager.clearStorage();
        //Indexes
        for (IndexProvider index : indexes.values()) index.clearStorage();
    }

    //############ Registered Storage Managers ##############

    private static final Map<String, String> REGISTERED_STORAGE_MANAGERS = new HashMap<String, String>() {{
        put("berkeleyje", "com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager");
        put("persistit", "com.thinkaurelius.titan.diskstorage.persistit.PersistitStoreManager");
        put("hazelcast", "com.thinkaurelius.titan.diskstorage.hazelcast.HazelcastCacheStoreManager");
        put("hazelcastcache", "com.thinkaurelius.titan.diskstorage.hazelcast.HazelcastCacheStoreManager");
        put("infinispan", "com.thinkaurelius.titan.diskstorage.infinispan.InfinispanCacheStoreManager");
        put("cassandra", "com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager");
        put("cassandrathrift", "com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager");
        put("astyanax", "com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager");
        put("hbase", "com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager");
        put("embeddedcassandra", "com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager");
        put("inmemory", "com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager");
    }};

    public static final Map<String, ConfigOption> REGISTERED_STORAGE_MANAGERS_SHORTHAND = new HashMap<String, ConfigOption>() {{
        put("berkeleyje", STORAGE_DIRECTORY);
        put("persistit", STORAGE_DIRECTORY);
        put("hazelcast", STORAGE_DIRECTORY);
        put("hazelcastcache", STORAGE_DIRECTORY);
        put("infinispan", STORAGE_DIRECTORY);
        put("cassandra", STORAGE_HOSTS);
        put("cassandrathrift", STORAGE_HOSTS);
        put("astyanax", STORAGE_HOSTS);
        put("hbase", STORAGE_HOSTS);
        put("embeddedcassandra", STORAGE_CONF_FILE);
        put("inmemory", null);
    }};

    private static final Map<String, String> REGISTERED_INDEX_PROVIDERS = new HashMap<String, String>() {{
        put("lucene", "com.thinkaurelius.titan.diskstorage.lucene.LuceneIndex");
        put("elasticsearch", "com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex");
        put("es", "com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex");
    }};

    private final Function<String, Locker> CONSISTENT_KEY_LOCKER_CREATOR = new Function<String, Locker>() {
        @Override
        public Locker apply(String lockerName) {
            KeyColumnValueStore lockerStore;
            try {
                lockerStore = getStore(lockerName);
            } catch (StorageException e) {
                throw new TitanConfigurationException("Could not retrieve store named " + lockerName + " for locker configuration", e);
            }
            return new ConsistentKeyLocker.Builder(lockerStore).fromConfig(configuration).build();
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

    static {
        Properties props;

        try {
            props = new Properties();
            InputStream in = TitanFactory.class.getClassLoader().getResourceAsStream(TitanConstants.TITAN_PROPERTIES_FILE);
            if (in != null && in.available() > 0) {
                props.load(in);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        registerShorthands(props, "storage.", REGISTERED_STORAGE_MANAGERS);
        registerShorthands(props, "index.", REGISTERED_INDEX_PROVIDERS);
    }

    public static final void registerShorthands(Properties props, String prefix, Map<String, String> shorthands) {
        for (String key : props.stringPropertyNames()) {
            if (key.toLowerCase().startsWith(prefix)) {
                String shorthand = key.substring(prefix.length()).toLowerCase();
                String clazz = props.getProperty(key);
                shorthands.put(shorthand, clazz);
                log.debug("Registering shorthand [{}] for [{}]", shorthand, clazz);
            }
        }
    }

//
//    public synchronized static final void registerStorageManager(String name, Class<? extends StoreManager> clazz) {
//        Preconditions.checkNotNull(name);
//        Preconditions.checkNotNull(clazz);
//        Preconditions.checkArgument(!StringUtils.isEmpty(name));
//        Preconditions.checkNotNull(!REGISTERED_STORAGE_MANAGERS.containsKey(name),"A storage manager has already been registered for name: " + name);
//        REGISTERED_STORAGE_MANAGERS.put(name,clazz);
//    }
//
//    public synchronized static final void removeStorageManager(String name) {
//        Preconditions.checkNotNull(name);
//        REGISTERED_STORAGE_MANAGERS.remove(name);
//    }

}
