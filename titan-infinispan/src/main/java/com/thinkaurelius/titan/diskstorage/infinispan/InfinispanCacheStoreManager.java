package com.thinkaurelius.titan.diskstorage.infinispan;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_CONF_FILE_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NAMESPACE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.common.NoOpStoreTransaction;
import com.thinkaurelius.titan.diskstorage.infinispan.ext.StaticBufferAE;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InfinispanCacheStoreManager extends LocalStoreManager implements CacheStoreManager {
    
    /**
     * Infinispan has a flat cache namespace. Titan forms cache names by
     * concatenating three strings:
     * 
     * <ol>
     * <li>Cache prefix string (the value of this configuration option)</li>
     * <li>A colon, i.e. ":"</li>
     * <li>Titan-internal store name, e.g. "edgestore"</li>
     * </ol>
     * 
     * Under the default configuration, an example cache name used by Titan
     * would be "titan:edgestore".
     * <p>
     * This configuration parameter can be set to any string. This is useful
     * when running multiple Titan graph databases against a single Infinispan
     * backend. To run logically separate Titan graph databases in a shared
     * Infinispan backend, assign each a unique cache prefix. If these logically
     * separate Titan graph databases were to use a shared Infinispan backend
     * and the same cache prefix, then they would catastrophically overwrite one
     * another.
     * <p>
     * Default = {@value #INFINISPAN_CACHE_NAME_PREFIX_DEFAULT}
     */
    public static final String INFINISPAN_CACHE_NAME_PREFIX_KEY = "cache-prefix";
    public static final String INFINISPAN_CACHE_NAME_PREFIX_DEFAULT = "titan";
    
    /**
     * The name of the Infinispan transaction manager lookup class to
     * instantiate. This can be set to a fully-qualified classname or a string
     * without any dots. If it is a string without any dots, then
     * {@value #INFINISPAN_TXLOOKUP_CLASS_PREFIX} and a dot are prepended at
     * runtime.
     * <p>
     * For more information about this setting and its possible values, see <a
     * href=
     * "http://infinispan.org/docs/6.0.x/user_guide/user_guide.html#_transactions"
     * >the "Transactions" section of the Infinispan manual.</a>
     * <p>
     * The configured class must have a public no-arg constructor and it must
     * implement the interface
     * {@link org.infinispan.transaction.lookup.TransactionManagerLookup}. A
     * class that doesn't meet these requirements will generate
     * {@code StorageException}s at runtime.
     * <p>
     * Default = {@value #INFINISPAN_TXLOOKUP_CLASS_DEFAULT}
     */
    public static final String INFINISPAN_TXLOOKUP_CLASS_KEY = "tx-mgr-lookup";
    public static final String INFINISPAN_TXLOOKUP_CLASS_DEFAULT = "JBossStandaloneJTAManagerLookup";
    
    /**
     * If the value of {@link #INFINISPAN_TXLOOKUP_CLASS_KEY} doesn't include a
     * dot, then this default packagename and a dot are prepended to the value.
     * 
     * @see #INFINISPAN_TXLOOKUP_CLASS_KEY
     */
    public static final String INFINISPAN_TXLOOKUP_CLASS_PREFIX = "org.infinispan.transaction.lookup";
    
    /**
     * The Infinispan locking mode to use. This setting has no effect when
     * {@value GraphDatabaseConfiguration#STORAGE_TRANSACTIONAL_KEY} is false.
     * <p>
     * The only valid settings for this property are the enum names for
     * {@link LockingMode}: "OPTIMISTIC" or "PESSIMISTIC".
     * <p>
     * Default = {@value #INFINISPAN_TX_LOCK_MODE_DEFAULT}
     */
    public static final String INFINISPAN_TX_LOCK_MODE_KEY = "lock-mode";
    public static final String INFINISPAN_TX_LOCK_MODE_DEFAULT = "OPTIMISTIC";
    
    /**
     * The number of milliseconds to wait when acquiring a lock before failing
     * the attempt. This setting has no effect when
     * {@value GraphDatabaseConfiguration#STORAGE_TRANSACTIONAL_KEY} is false.
     * <p>
     * Default = {@value #INFINISPAN_TX_LOCK_ACQUIRE_TIMEOUT_MS_DEFAULT}
     */
    public static final String INFINISPAN_TX_LOCK_ACQUIRE_TIMEOUT_MS_KEY = "lock-acquire-ms";
    public static final long INFINISPAN_TX_LOCK_ACQUIRE_TIMEOUT_MS_DEFAULT = 30000L; // 10000 is infinispan default
    
    /**
     * Path to an Infinispan SingleFileCacheStore. Null disables persistence. If
     * the value is non-null, then it must be a valid filesystem path.
     * <p>
     * Default = {@value #INFINISPAN_SINGLE_FILE_STORE_PATH_KEY}
     */
    public static final String INFINISPAN_SINGLE_FILE_STORE_PATH_KEY = "single-file-store-path";
    public static final String INFINISPAN_SINGLE_FILE_STORE_PATH_DEFAULT = null;
    
    /**
     * Whether to propagates writes from the cache to the SingleFileCacheStore
     * asynchronously (write-behind persistence) or synchronously (write-through
     * persistence). True sets async persistence. False sets synchronous
     * persistence (i.e. writes to the cache will block until the write has been
     * duplicated on disk).
     * <p>
     * This setting has no effect when
     * {@value #INFINISPAN_SINGLE_FILE_STORE_PATH_KEY} is null.
     * <p>
     * Default = {@value #INFINISPAN_SINGLE_FILE_STORE_ASYNC_DEFAULT}
     */
    public static final String INFINISPAN_SINGLE_FILE_STORE_ASYNC_KEY = "single-file-store-async";
    public static final boolean INFINISPAN_SINGLE_FILE_STORE_ASYNC_DEFAULT = false;
    
    /**
     * Whether to preload data from the single file store on startup. True
     * preloads file store data into memory on startup. False does not preload
     * (lazy loading).
     * <p>
     * This setting has no effect when
     * {@value #INFINISPAN_SINGLE_FILE_STORE_PATH_KEY} is null.
     * <p>
     * Default = {@value #INFINISPAN_SINGLE_FILE_STORE_PRELOAD_DEFAULT}
     */
    public static final String INFINISPAN_SINGLE_FILE_STORE_PRELOAD_KEY = "single-file-store-preload";
    public static final boolean INFINISPAN_SINGLE_FILE_STORE_PRELOAD_DEFAULT = false;
    
    private static final Logger log = LoggerFactory.getLogger(InfinispanCacheStoreManager.class);
    
    protected final StoreFeatures features = getDefaultFeatures();
    
    private final EmbeddedCacheManager manager;
    private final String cacheNamePrefix;
    private final TransactionManagerLookup txlookup;
    private final LockingMode lockmode;
    private final long lockAcquisitionTimeoutMS;
    private final String singleFileStorePath;
    private final boolean singleFileStoreAsync;
    private final boolean singleFileStorePreload;
    
    public InfinispanCacheStoreManager(Configuration config) throws StorageException {
        super(config);
        
        // TODO make these hacks configurable
        System.setProperty("JTAEnvironmentBean.jtaTMImplementation", "com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple");
        System.setProperty("JTAEnvironmentBean.jtaUTImplementation","com.arjuna.ats.internal.jta.transaction.arjunacore.UserTransactionImple");
        System.setProperty("com.arjuna.ats.arjuna.coordinator.defaultTimeout", "600");
        
        String xmlFile = config.getString(STORAGE_CONF_FILE_KEY, null);
        if (null == xmlFile) {
            manager = getManagerWithStandardConfig();
        } else {
            manager = getManagerWithXmlConfig(xmlFile);
        }
        
        if (manager.getStatus().equals(ComponentStatus.INSTANTIATED)) // TODO this is probably the wrong way to do this because it doesn't account for concurrency... investiagte whether start() is internally threadsafe
            manager.start();
        
        // Transaction manager lookup class
        String txl = config.getString(INFINISPAN_TXLOOKUP_CLASS_KEY, INFINISPAN_TXLOOKUP_CLASS_DEFAULT);
        if (null == txl || txl.trim().isEmpty()) {
            throw new PermanentStorageException(INFINISPAN_TXLOOKUP_CLASS_KEY + " must be non-null and non-empty");
        }
        if (!txl.contains(".")) {
            txl = INFINISPAN_TXLOOKUP_CLASS_PREFIX + "." + txl;
        }
        Preconditions.checkArgument(!txl.isEmpty());
        Preconditions.checkArgument(txl.contains("."));
        txlookup = instantiateStandardTransactionManagerLookup(txl);

        // Locking mode
        LockingMode lm = LockingMode.valueOf(INFINISPAN_TX_LOCK_MODE_DEFAULT);
        String rawLM = config.getString(INFINISPAN_TX_LOCK_MODE_KEY, INFINISPAN_TX_LOCK_MODE_DEFAULT);
        try {
            lm = LockingMode.valueOf(rawLM);
        } catch (NullPointerException e) {
            log.error("Could not parse Infinispan locking mode configuration, using default {}", INFINISPAN_TX_LOCK_MODE_DEFAULT, e);
        } catch (IllegalArgumentException e) {
            log.error("Unrecognized locking mode string {}, using default", lm, INFINISPAN_TX_LOCK_MODE_DEFAULT, e);
        }
        lockmode = lm;
        Preconditions.checkNotNull(lockmode);
        
        // Lock acquistiion timeout
        lockAcquisitionTimeoutMS =
                config.getLong(INFINISPAN_TX_LOCK_ACQUIRE_TIMEOUT_MS_KEY,
                               INFINISPAN_TX_LOCK_ACQUIRE_TIMEOUT_MS_DEFAULT);
        
        // Persistence
        singleFileStorePath =
                config.getString(INFINISPAN_SINGLE_FILE_STORE_PATH_KEY,
                                 INFINISPAN_SINGLE_FILE_STORE_PATH_DEFAULT);
        singleFileStoreAsync =
                config.getBoolean(INFINISPAN_SINGLE_FILE_STORE_ASYNC_KEY,
                                  INFINISPAN_SINGLE_FILE_STORE_ASYNC_DEFAULT);

        singleFileStorePreload =
                config.getBoolean(INFINISPAN_SINGLE_FILE_STORE_PRELOAD_KEY,
                                  INFINISPAN_SINGLE_FILE_STORE_PRELOAD_DEFAULT);
        
        
        cacheNamePrefix = config.getString(INFINISPAN_CACHE_NAME_PREFIX_KEY, INFINISPAN_CACHE_NAME_PREFIX_DEFAULT);
    }
    
    @Override
    public synchronized CacheStore openDatabase(final String storeName) throws StorageException {
        
        final String fullCacheName = cacheNamePrefix + ":" + storeName;
        
        org.infinispan.configuration.cache.Configuration conf =
                manager.getCacheConfiguration(fullCacheName);
        
        if (null == conf) {
            defineStandardCacheConfig(fullCacheName);
        } else {
            log.info("Using existing config for cache {}; Titan configuration property {}.{} is ignored for this cache",
                    new Object[] { fullCacheName, STORAGE_NAMESPACE, STORAGE_TRANSACTIONAL_KEY });
            log.debug("Existing config for cache {}: {}", fullCacheName, conf);
        }
        
        final InfinispanCacheStore newStore;
        final Cache<?,?> c = manager.getCache(fullCacheName);
        if (c.getCacheConfiguration().transaction().transactionMode().equals(TransactionMode.TRANSACTIONAL)) {
            newStore = new InfinispanCacheTransactionalStore(fullCacheName, storeName, manager);
        } else {
            newStore = new InfinispanCacheStore(fullCacheName, storeName, manager);
        }
        
        
        return newStore;
    }

    @Override
    public void clearStorage() throws StorageException {
        
        for (String fullName : manager.getCacheNames()) {
            if (fullName.startsWith(cacheNamePrefix + ":")) {
                String storeName = fullName.replaceFirst("^" + Pattern.quote(cacheNamePrefix + ":"), "");
                CacheStore cs = openDatabase(storeName);
                cs.clearStore();
            }
        }
        // close();
    }

    @Override
    public String getName() {
        return toString();
    }


    @Override
    public StoreTransaction beginTransaction(StoreTxConfig config) throws StorageException {
        if (transactional) {
            return new InfinispanCacheTransaction(config);
        } else {
            return new NoOpStoreTransaction(config);
        }
    }

    @Override
    public void close() throws StorageException {
        manager.stop(); // Stops all of the manager's caches
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    private StoreFeatures getDefaultFeatures() {
        StoreFeatures features = new StoreFeatures();

        features.supportsOrderedScan = false;
        features.supportsUnorderedScan = true;
        features.supportsBatchMutation = false;
        features.supportsMultiQuery = false;

        features.supportsTransactions = true;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;

        /*
         * isDistributed, isKeyOrdered, and hasLocalKeyPartition should
         * technically assume different values depending on Infinispan's
         * configured clustering mode.
         * 
         * local mode:
         * http://infinispan.org/docs/6.0.x/user_guide/user_guide.html#_local_mode
         * 
         * isDistributed = false;
         * isKeyOrdered = false;
         * hasLocalKeyPartition = true;
         * 
         * 
         * replicated mode:
         * http://infinispan.org/docs/6.0.x/user_guide/user_guide.html#_replicated_mode
         * 
         * isDistributed = false; // this is semantically debatable, but false seems the best fit based on how it affects partitioning
         * isKeyOrdered = false;
         * hasLocalKeyPartition = true;
         * 
         * 
         * distributed mode:
         * http://infinispan.org/docs/6.0.x/user_guide/user_guide.html#_distribution_mode
         * 
         * isDistributed = true;
         * isKeyOrdered = false;
         * hasLocalKeyPartition = true; // true, but not sure how we would support it at a higher level
         * 
         */
        features.isDistributed = true; 
        features.isKeyOrdered = false;
        features.hasLocalKeyPartition = false;

        return features;
    }
    
    /**
     * Defines a new Infinispan cache using the supplied name. The cache
     * configuration is the manager's default cache configuration plus
     * transactions explicitly either enabled or disabled depending on whether
     * the {@code transactional} field is true or false.
     * <p>
     * <b>The named cache must not already exist.</b>
     * 
     * @param cachename the cache to define
     */
    private void defineStandardCacheConfig(String cachename) throws StorageException {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        
        // This method should only be called for caches with no existing config
        Preconditions.checkArgument(null == manager.getCacheConfiguration(cachename));
        
        // Load default cache configuration
        cb.read(manager.getDefaultCacheConfiguration());
        
        if (transactional) {
            cb.transaction()
              .transactionMode(TransactionMode.TRANSACTIONAL)
              .transactionManagerLookup(txlookup)
              .lockingMode(lockmode)
              .autoCommit(false)
              .locking()
              .lockAcquisitionTimeout(lockAcquisitionTimeoutMS, TimeUnit.MILLISECONDS)
              .build();
        } else {
            cb.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
        }
        
        if (null != singleFileStorePath) {
            cb.persistence()
              .addSingleFileStore()
              .preload(singleFileStorePreload)
              .location(singleFileStorePath)
              .async()
              .enabled(singleFileStoreAsync);
        }
        
        manager.defineConfiguration(cachename, cb.build());
        
        log.info("Defined transactional={} configuration for cache {}", transactional, cachename);
    }
    
    private static TransactionManagerLookup instantiateStandardTransactionManagerLookup(String classname) throws PermanentStorageException {
        Class<?> c;
        try {
            c = Class.forName(classname);
        } catch (ClassNotFoundException e) {
            throw new PermanentStorageException(e);
        }
        
        try {
            TransactionManagerLookup tml = (TransactionManagerLookup)c.newInstance();
            return tml;
        } catch (SecurityException e) {
            throw new PermanentStorageException(e);
        } catch (InstantiationException e) {
            throw new PermanentStorageException(e);
        } catch (IllegalAccessException e) {
            throw new PermanentStorageException(e);
        }
    }

    /**
     * Constructs an {@code EmbeddedCacheManager} using the default Infinispan
     * GlobalConfiguration with one modification: duplicate JMX statistics
     * domains are allowed.
     * 
     * @return new cache manager
     */
    private EmbeddedCacheManager getManagerWithStandardConfig() {
        GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
        gcb.globalJmxStatistics().allowDuplicateDomains(true);
        gcb.serialization().addAdvancedExternalizer(new StaticBufferAE());
        EmbeddedCacheManager m = new DefaultCacheManager(gcb.build());
        log.info("Loaded ISPN cache manager using standard GlobalConfiguration");
        log.debug("Standard global configuration: {}",
                m.getGlobalComponentRegistry().getGlobalConfiguration());
        return m;
    }

    /**
     * Constructs an {@code EmbeddedCacheManager} using the Infinispan XML
     * configuration file at the supplied path.
     * 
     * @param xmlpath path to infinispan.xml
     * @return new cache manager
     * @throws PermanentStorageException if the xml file could not be read
     */
    private EmbeddedCacheManager getManagerWithXmlConfig(String xmlpath) throws PermanentStorageException {
        try {
            EmbeddedCacheManager m = new DefaultCacheManager(xmlpath);
            log.info("Loaded ISPN cache manager using config file {}", xmlpath);
            return m;
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }
    }
}
