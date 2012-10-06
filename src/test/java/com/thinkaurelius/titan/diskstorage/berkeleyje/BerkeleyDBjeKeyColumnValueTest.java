package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManagerAdapter;
import org.apache.commons.configuration.Configuration;

import static junit.framework.Assert.assertEquals;


public class BerkeleyDBjeKeyColumnValueTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = StorageSetup.getBerkeleyJEStorageConfiguration();
        config.subset(KeyValueStoreManagerAdapter.KEYLENGTH_NAMESPACE).setProperty(storeName,8);

        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(config);
        KeyValueStoreManagerAdapter smadapter = new KeyValueStoreManagerAdapter(sm,config);

        //assertEquals(8,((KeyValueStoreAdapter)smadapter.openDatabase(storeName)).getKeyLength());
        return smadapter;
	}


}
