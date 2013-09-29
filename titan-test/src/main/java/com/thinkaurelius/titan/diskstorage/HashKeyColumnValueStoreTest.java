package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.indexing.HashPrefixKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class HashKeyColumnValueStoreTest extends KeyColumnValueStoreTest {

    @Override
    public void open() throws StorageException {
        manager = openStorageManager();
        store = new HashPrefixKeyColumnValueStore(manager.openDatabase(storeName), 4);
        tx = startTx();
    }

}
