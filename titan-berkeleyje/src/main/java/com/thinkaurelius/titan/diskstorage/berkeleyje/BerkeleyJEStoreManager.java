package com.thinkaurelius.titan.diskstorage.berkeleyje;


import com.google.common.base.Preconditions;
import com.sleepycat.je.*;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.MergedConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.util.system.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.diskstorage.configuration.ConfigOption.disallowEmpty;

@PreInitializeConfigOptions
public class BerkeleyJEStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJEStoreManager.class);

    public static final ConfigOption<Integer> JVM_CACHE = new ConfigOption<Integer>(GraphDatabaseConfiguration.STORAGE_NS,"cache-percentage",
            "Percentage of JVM heap reserved for BerkeleyJE's cache",
            ConfigOption.Type.MASKABLE, 65, ConfigOption.positiveInt());
//    public static final String CACHE_KEY = "cache-percentage";
//    public static final int CACHE_DEFAULT = 65;

    public static final ConfigOption<LockMode> LOCK_MODE = new ConfigOption<LockMode>(GraphDatabaseConfiguration.STORAGE_NS, "lock-mode",
            "The BDB record lock mode used for read operations",
            ConfigOption.Type.MASKABLE, LockMode.class, LockMode.DEFAULT, disallowEmpty(LockMode.class));

    public static final ConfigOption<IsolationLevel> ISOLATION_LEVEL = new ConfigOption<IsolationLevel>(GraphDatabaseConfiguration.STORAGE_NS, "isolation-level",
            "The isolation level used by transactions",
            ConfigOption.Type.MASKABLE,  IsolationLevel.class, IsolationLevel.REPEATABLE_READ, disallowEmpty(IsolationLevel.class));

    private final Map<String, BerkeleyJEKeyValueStore> stores;

    protected Environment environment;
    protected final StoreFeatures features;

    public BerkeleyJEStoreManager(Configuration configuration) throws StorageException {
        super(configuration);
        stores = new HashMap<String, BerkeleyJEKeyValueStore>();

        int cachePercentage = configuration.get(JVM_CACHE);
        initialize(cachePercentage);

        features = new StandardStoreFeatures.Builder()
                    .orderedScan(true)
                    .transactional(transactional)
                    .keyConsistent(GraphDatabaseConfiguration.buildConfiguration())
                    .locking(true)
                    .keyOrdered(true)
                    .build();

//        features = new StoreFeatures();
//        features.supportsOrderedScan = true;
//        features.supportsUnorderedScan = false;
//        features.supportsBatchMutation = false;
//        features.supportsTxIsolation = transactional;
//        features.supportsConsistentKeyOperations = true;
//        features.supportsLocking = true;
//        features.isKeyOrdered = true;
//        features.isDistributed = false;
//        features.hasLocalKeyPartition = false;
//        features.supportsMultiQuery = false;
    }

    private void initialize(int cachePercent) throws StorageException {
        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(transactional);
            envConfig.setCachePercent(cachePercent);

            if (batchLoading) {
                envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
                envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
            }

            //Open the environment
            environment = new Environment(directory, envConfig);


        } catch (DatabaseException e) {
            throw new PermanentStorageException("Error during BerkeleyJE initialization: ", e);
        }

    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BerkeleyJETx beginTransaction(final BaseTransactionConfig txCfg) throws StorageException {
        try {
            Transaction tx = null;

            Configuration effectiveCfg =
                    new MergedConfiguration(txCfg.getCustomOptions(), getStorageConfig());

            if (transactional) {
                TransactionConfig txnConfig = new TransactionConfig();
                effectiveCfg.get(ISOLATION_LEVEL).configure(txnConfig);
                tx = environment.beginTransaction(null, txnConfig);
            }
            BerkeleyJETx btx = new BerkeleyJETx(tx, effectiveCfg.get(LOCK_MODE), txCfg);

            if (log.isTraceEnabled()) {
                log.trace("Berkeley tx created", new TransactionBegin(btx.toString()));
            }

            return btx;
        } catch (DatabaseException e) {
            throw new PermanentStorageException("Could not start BerkeleyJE transaction", e);
        }
    }

    @Override
    public BerkeleyJEKeyValueStore openDatabase(String name) throws StorageException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            BerkeleyJEKeyValueStore store = stores.get(name);
            return store;
        }
        try {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(isReadOnly);
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(transactional);

            dbConfig.setKeyPrefixing(true);

            if (batchLoading) {
                dbConfig.setDeferredWrite(true);
            }

            Database db = environment.openDatabase(null, name, dbConfig);

            log.debug("Opened database {}", name, new Throwable());

            BerkeleyJEKeyValueStore store = new BerkeleyJEKeyValueStore(name, db, this);
            stores.put(name, store);
            return store;
        } catch (DatabaseException e) {
            throw new PermanentStorageException("Could not open BerkeleyJE data store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws StorageException {
        for (Map.Entry<String,KVMutation> muts : mutations.entrySet()) {
            BerkeleyJEKeyValueStore store = openDatabase(muts.getKey());
            KVMutation mut = muts.getValue();

            if (!mut.hasAdditions() && !mut.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", muts.getKey());
            } else {
                log.debug("Mutating {}", muts.getKey());
            }

            if (mut.hasAdditions()) {
                for (KeyValueEntry entry : mut.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh);
                    log.trace("Insertion on {}: {}", muts.getKey(), entry);
                }
            }
            if (mut.hasDeletions()) {
                for (StaticBuffer del : mut.getDeletions()) {
                    store.delete(del,txh);
                    log.trace("Deletion on {}: {}", muts.getKey(), del);
                }
            }
        }
    }

    void removeDatabase(BerkeleyJEKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }


    @Override
    public void close() throws StorageException {
        if (environment != null) {
            if (!stores.isEmpty())
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            try {
                // TODO this looks like a race condition
                //Wait just a little bit before closing so that independent transaction threads can clean up.
                Thread.sleep(30);
            } catch (InterruptedException e) {
                //Ignore
            }
            try {
                environment.close();
            } catch (DatabaseException e) {
                throw new PermanentStorageException("Could not close BerkeleyJE database", e);
            }
        }

    }

    @Override
    public void clearStorage() throws StorageException {
        if (!stores.isEmpty())
            throw new IllegalStateException("Cannot delete store, since database is open: " + stores.keySet().toString());

        Transaction tx = null;
        for (String db : environment.getDatabaseNames()) {
            environment.removeDatabase(tx, db);
            log.debug("Removed database {} (clearStorage)", db);
        }
        close();
        IOUtils.deleteFromDirectory(directory);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }


    public static enum IsolationLevel {
        READ_UNCOMMITTED {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setReadUncommitted(true);
            }
        }, READ_COMMITTED {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setReadCommitted(true);

            }
        }, REPEATABLE_READ {
            @Override
            void configure(TransactionConfig cfg) {
                // This is the default and has no setter
            }
        }, SERIALIZABLE {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setSerializableIsolation(true);
            }
        };

        abstract void configure(TransactionConfig cfg);
    };

    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }
}
