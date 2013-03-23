package com.thinkaurelius.titan.diskstorage.persistit;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.util.interval.Range;

public class PersistitKeyColumnValueTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        PersistitStoreManager sm = new PersistitStoreManager();
        KeyValueStoreManagerAdapter smadapter = new KeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName, 8));
        return smadapter;
    }

}
