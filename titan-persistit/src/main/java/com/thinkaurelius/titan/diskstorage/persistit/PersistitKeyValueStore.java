package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;

public class PersistitKeyValueStore implements KeyValueStore {

    private final String name;
    private final PersistitStoreManager storeManager;
    private final Exchange exchange;

    public PersistitKeyValueStore(String n, PersistitStoreManager mgr, Persistit db) throws StorageException {
        name = n;
        storeManager = mgr;

        try {
            exchange = db.getExchange("titan", name, true);
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        }
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Clears the contents of this kv store
     */
    public void clear() {
        exchange.clear();
    }
}
