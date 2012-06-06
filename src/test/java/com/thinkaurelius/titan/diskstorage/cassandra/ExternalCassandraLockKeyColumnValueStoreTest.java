package com.thinkaurelius.titan.diskstorage.cassandra;

import org.apache.commons.configuration.Configuration;
import org.junit.After;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalCassandraLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper("127.0.0.1");

    @Override
    public void cleanUp() {
        StorageSetup.deleteHomeDir();
        ch.startCassandra();
        CassandraThriftStorageManager cmanager = new CassandraThriftStorageManager(CassandraLocalhostHelper.getLocalStorageConfiguration());
        cmanager.dropKeyspace(CassandraThriftStorageManager.KEYSPACE_DEFAULT);
    }

    @Override
    public StorageManager openStorageManager(short idx) {
    	Configuration sc = CassandraLocalhostHelper.getLocalStorageConfiguration();
    	sc.addProperty(CassandraThriftStorageManager.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "cassandra-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	
        return new CassandraThriftStorageManager(sc);
    }

	
	@After
	public void cassandraTearDown() {
		ch.stopCassandra();
	}
}
