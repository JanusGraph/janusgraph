package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;
import org.apache.commons.configuration.Configuration;

import static junit.framework.Assert.assertEquals;


public class BerkeleyDBjeKeyColumnValueTest extends KeyColumnValueStoreTest {

    public StorageManager openStorageManager() {
        Configuration config = StorageSetup.getBerkeleyJEStorageConfiguration();
        config.subset(KeyValueStorageManagerAdapter.KEYLENGTH_NAMESPACE).setProperty(storeName,8);

        BerkeleyJEStorageManager sm = new BerkeleyJEStorageManager(config);
        KeyValueStorageManagerAdapter smadapter = new KeyValueStorageManagerAdapter(sm,config);

        //assertEquals(8,((KeyValueStoreAdapter)smadapter.openDatabase(storeName)).getKeyLength());
        return smadapter;
	}


}
