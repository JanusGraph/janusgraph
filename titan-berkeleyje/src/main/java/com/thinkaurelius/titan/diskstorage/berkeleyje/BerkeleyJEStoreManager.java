package com.thinkaurelius.titan.diskstorage.berkeleyje;


import com.google.common.base.Preconditions;
import com.sleepycat.je.*;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.util.system.IOUtils;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BerkeleyJEStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJEStoreManager.class);

    public static final String CACHE_KEY = "cache-percentage";
    public static final int CACHE_DEFAULT = 65;

    private final Map<String, BerkeleyJEKeyValueStore> stores;

    protected Environment environment;
    protected final StoreFeatures features;

    public BerkeleyJEStoreManager(Configuration configuration) throws StorageException {
        super(configuration);
        stores = new HashMap<String, BerkeleyJEKeyValueStore>();
        if (!transactional)
            log.warn("Transactions are disabled. Ensure that there is at most one Titan instance interacting with this BerkeleyDB instance, otherwise your database may corrupt.");

        int cachePercentage = configuration.getInt(CACHE_KEY, CACHE_DEFAULT);
        initialize(cachePercentage);

        features = new StoreFeatures();
        features.supportsOrderedScan = true;
        features.supportsUnorderedScan = false;
        features.supportsBatchMutation = false;
        features.supportsTransactions = true;
        features.supportsConsistentKeyOperations = false;
        features.supportsLocking = true;
        features.isKeyOrdered = true;
        features.isDistributed = false;
        features.hasLocalKeyPartition = false;
        features.supportsMultiQuery = false;
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
    public BerkeleyJETx beginTransaction(final StoreTxConfig config) throws StorageException {
        try {
            Transaction tx = null;
            if (transactional) {
                tx = environment.beginTransaction(null, null);
            }
            return new BerkeleyJETx(tx, config);
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
            BerkeleyJEKeyValueStore store = new BerkeleyJEKeyValueStore(name, db, this);
            stores.put(name, store);
            return store;
        } catch (DatabaseException e) {
            throw new PermanentStorageException("Could not open BerkeleyJE data store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    void removeDatabase(BerkeleyJEKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
        }
        stores.remove(db.getName());
    }


    @Override
    public void close() throws StorageException {
        if (environment != null) {
            if (!stores.isEmpty())
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            try {
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
        }
        close();
        IOUtils.deleteFromDirectory(directory);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }
}
