package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.Test;


public class BerkeleyDBjeKeyColumnValueTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyJeStorageSetup.getBerkeleyJEStorageConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName, 8));
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
