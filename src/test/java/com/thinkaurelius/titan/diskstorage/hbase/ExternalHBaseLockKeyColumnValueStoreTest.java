package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalHBaseLockKeyColumnValueStoreTest
	extends LockKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager(short idx) throws StorageException {
    	Configuration sc = StorageSetup.getHBaseStorageConfiguration();
    	sc.addProperty(ConsistentKeyLockStore.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "hbase-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	sc.addProperty(GraphDatabaseConfiguration.LOCK_EXPIRE_MS, EXPIRE_MS);
        return new HBaseStorageManager(sc);
    }
}
