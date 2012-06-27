package com.thinkaurelius.titan.diskstorage.hbase;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalHBaseLockKeyColumnValueStoreTest
	extends LockKeyColumnValueStoreTest {

    public StorageManager openStorageManager(short idx) {
    	Configuration sc = StorageSetup.getHBaseStorageConfiguration();
    	sc.addProperty(HBaseStorageManager.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "hbase-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
        return new HBaseStorageManager(sc);
    }
}
