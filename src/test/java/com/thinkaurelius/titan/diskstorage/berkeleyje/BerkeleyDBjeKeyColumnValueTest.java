package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManagerAdapter;
import org.apache.commons.configuration.Configuration;

import static junit.framework.Assert.assertEquals;


public class BerkeleyDBjeKeyColumnValueTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(StorageSetup.getBerkeleyJEStorageConfiguration());
        KeyValueStoreManagerAdapter smadapter = new KeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName,8));
        return smadapter;
	}


}
