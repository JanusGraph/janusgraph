package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class PersistitStoreManager implements KeyValueStoreManager {

    private final Map<String, PersistitKeyValueStore> stores;
    private static final StoreFeatures features = new StoreFeatures();
    static {
        features.supportsTransactions = true;
        features.isDistributed = false;

        //@todo: figure out what these do, Copied from Berkeley for now
        features.supportsScan = true;
        features.supportsBatchMutation = false;
        features.supportsConsistentKeyOperations = false;
        features.supportsLocking = true;
        features.isKeyOrdered = true;
        features.hasLocalKeyPartition = false;
    }

    private Persistit db;
    private Exchange exchange;

    public PersistitStoreManager() throws StorageException {
        //@todo: take some config data here
        stores = new HashMap<String, PersistitKeyValueStore>();
        db = new Persistit();

        try {
            db.initialize();
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        }
    }

    /**
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     * <p/>
     * By default, the key length will be variable
     *
     * @param name Name of database
     * @return Database Handle
     * @throws com.thinkaurelius.titan.diskstorage.StorageException
     *
     */
    @Override
    public PersistitKeyValueStore openDatabase(String name) throws StorageException {
        if (stores.containsKey(name)) {
            return stores.get(name);
        }

        PersistitKeyValueStore store = new PersistitKeyValueStore(name, this, db);
        stores.put(name, store);
        return store;
    }

    @Override
    public void close() throws StorageException {
        if (db != null) {
            if (!stores.isEmpty()) {
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            }
            try {
                db.close(true);
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex.toString());
            }
        }
    }

    /**
     * Returns a transaction handle for a new transaction.
     *
     * @return New Transaction Handle
     */
    @Override
    public PersistitTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
        //all Exchanges created by a thread share the same transaction context
        Transaction tx = db.getTransaction();
        return new PersistitTransaction(tx, level);
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public void clearStorage() throws StorageException {
        for(String key : stores.keySet()) {
            PersistitKeyValueStore store = stores.remove(key);
            store.clear();
        }
        //@todo: delete storage directory
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        //@todo: this
        throw new PermanentStorageException("write this part");
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        //@todo: this
        throw new PermanentStorageException("write this part");
    }
}
