package com.thinkaurelius.titan.diskstorage.infinispan;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_CONF_FILE_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NAMESPACE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.common.NoOpStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InfinispanCacheStoreManager extends LocalStoreManager implements CacheStoreManager {
    
    public static final String INFINISPAN_CACHE_NAME_PREFIX_KEY = "cacheprefix";
    public static final String INFINISPAN_CACHE_NAME_PREFIX_DEFAULT = "titan";
    
    private static final Logger log = LoggerFactory.getLogger(InfinispanCacheStoreManager.class);
    
    protected final StoreFeatures features = getDefaultFeatures();
    
    private final EmbeddedCacheManager manager;
    private final String cacheNamePrefix;

    public InfinispanCacheStoreManager(Configuration config) throws StorageException {
        super(config);
        
        String xmlFile = config.getString(STORAGE_CONF_FILE_KEY, null);
        if (null == xmlFile) {
            manager = getManagerWithStandardConfig();
        } else {
            manager = getManagerWithXmlConfig(xmlFile);
        }
        
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
        
        InfinispanCacheStore newStore = new InfinispanCacheStore(fullCacheName, storeName, manager);
        return newStore;
    }

    @Override
    public void clearStorage() throws StorageException {
        
        for (String storeName : manager.getCacheNames()) {
            Cache<Object, Object> store = manager.getCache(storeName);
            store.clear();
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
//            TransactionManager tm = manager.getGlobalComponentRegistry().getComponent(TransactionManager.class);
//            http://infinispan.org/docs/6.0.x/getting_started/getting_started.html#_cache_with_transaction_management
            throw new UnsupportedOperationException();
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

        features.supportsTransactions = false;
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
    private void defineStandardCacheConfig(String cachename) {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        
        // This method should only be called for caches with no existing config
        Preconditions.checkArgument(null == manager.getCacheConfiguration(cachename));
        
        // Load default cache configuration
        cb.read(manager.getDefaultCacheConfiguration());
        
        if (transactional) {
            cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
                    .autoCommit(false).transactionManagerLookup(new DummyTransactionManagerLookup())
                    .build();
        } else {
            cb.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL).build();
        }
        
        manager.defineConfiguration(cachename, cb.build());
        
        log.info("Defined transactional={} configuration for cache {}", transactional, cachename);
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
        EmbeddedCacheManager m = new DefaultCacheManager(gcb.globalJmxStatistics().allowDuplicateDomains(true).build());
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
