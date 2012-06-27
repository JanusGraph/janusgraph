package com.thinkaurelius.titan.diskstorage.astyanax;

import com.thinkaurelius.titan.StorageSetup;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.testutil.CassandraUtil;

public class ExternalAstyanaxLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {
	
    @Override
    public StorageManager openStorageManager(short idx) {
    	Configuration sc = StorageSetup.getCassandraStorageConfiguration();
    	sc.addProperty(CassandraThriftStorageManager.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "astyanax-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	
        return new AstyanaxStorageManager(sc);
    }
}
