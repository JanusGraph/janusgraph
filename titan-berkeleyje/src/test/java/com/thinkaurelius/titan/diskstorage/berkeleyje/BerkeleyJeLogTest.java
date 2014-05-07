package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.diskstorage.log.KCVSLogTest;
import org.junit.Test;


public class BerkeleyJeLogTest extends KCVSLogTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyJeStorageSetup.getBerkeleyJEConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

}
