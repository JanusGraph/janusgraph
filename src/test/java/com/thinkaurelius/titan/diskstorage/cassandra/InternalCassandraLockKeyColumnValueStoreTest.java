package com.thinkaurelius.titan.diskstorage.cassandra;

import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class InternalCassandraLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

	@BeforeClass
	public static void startCassandra() {
    	CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
	}
    
    @Override
    public StorageManager openStorageManager(short idx) {
    	Configuration sc = StorageSetup.getCassandraStorageConfiguration();
    	sc.addProperty(CassandraThriftStorageManager.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "cassandra-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	
        return new CassandraThriftStorageManager(sc);
    }
}
