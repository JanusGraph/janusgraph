package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.diskstorage.log.KCVSLogTest;


public class BerkeleyLogTest extends KCVSLogTest {

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

}
