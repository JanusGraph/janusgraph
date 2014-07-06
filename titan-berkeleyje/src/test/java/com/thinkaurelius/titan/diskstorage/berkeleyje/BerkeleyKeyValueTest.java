package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.KeyValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;


public class BerkeleyKeyValueTest extends KeyValueStoreTest {

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration());
    }


}
