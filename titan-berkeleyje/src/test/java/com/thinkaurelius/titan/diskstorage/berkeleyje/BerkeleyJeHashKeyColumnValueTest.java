package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.diskstorage.HashKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.apache.commons.configuration.Configuration;

public class BerkeleyJeHashKeyColumnValueTest extends HashKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = BerkeleyJeStorageSetup.getBerkeleyJEStorageConfiguration();
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(config);

        // prefixed store doesn't support scan, because prefix is hash of a key which makes it un-ordered
        sm.features.supportsScan = false;

        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Override
    public void testGetKeysWithSliceQuery() throws Exception {
        // This is not going to work as getKeys(SliceQuery) is not supported by ordered store and
        // as this store is going to be prefixed it also means that we can't use getKeys(KeyRangeQuery)
    }
}
