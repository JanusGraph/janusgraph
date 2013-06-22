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
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }
}
