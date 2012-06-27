package com.thinkaurelius.titan.diskstorage.cassandra;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalCassandraLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    @Override
    public StorageManager openStorageManager(short idx) {
    	Configuration sc = StorageSetup.getCassandraStorageConfiguration();
    	sc.addProperty(CassandraThriftStorageManager.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "cassandra-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	
        return new CassandraThriftStorageManager(sc);
    }


    @BeforeClass
    public static void startCassandra() {
        ch.startCassandra();
    }

    @AfterClass
    public static void stopCassandra() {
        ch.stopCassandra();
    }
}
