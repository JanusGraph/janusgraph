package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.lucene.HashPrefixKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public abstract class HashKeyColumnValueStoreTest extends KeyColumnValueStoreTest {

    @Override
    public void open() throws StorageException {
        manager = openStorageManager();
        tx = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        store = new HashPrefixKeyColumnValueStore(manager.openDatabase(storeName), 4);
    }

}
