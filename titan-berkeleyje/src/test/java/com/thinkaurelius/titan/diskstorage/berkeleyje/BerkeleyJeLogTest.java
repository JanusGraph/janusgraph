package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.Test;


public class BerkeleyJeLogTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyJeStorageSetup.getBerkeleyJEConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
