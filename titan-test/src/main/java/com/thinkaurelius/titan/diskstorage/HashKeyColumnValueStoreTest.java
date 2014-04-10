package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.indexing.HashPrefixKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.indexing.HashPrefixStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class HashKeyColumnValueStoreTest extends KeyColumnValueStoreTest {

    @Override
    public void open() throws StorageException {
        manager = new HashPrefixStoreManager(openStorageManager(), HashPrefixKeyColumnValueStore.HashLength.SHORT);
        store = manager.openDatabase(storeName);
        tx = startTx();
    }

}
