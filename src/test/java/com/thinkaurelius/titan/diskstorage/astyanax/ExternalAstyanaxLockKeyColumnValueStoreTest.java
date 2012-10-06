package com.thinkaurelius.titan.diskstorage.astyanax;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStorageManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalAstyanaxLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {
	
    @Override
    public KeyColumnValueStoreManager openStorageManager(short idx) throws StorageException {
    	Configuration sc = StorageSetup.getCassandraStorageConfiguration();
    	sc.addProperty(ConsistentKeyLockStore.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "astyanax-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	sc.addProperty(GraphDatabaseConfiguration.LOCK_EXPIRE_MS, EXPIRE_MS);
    	
        return new AstyanaxStorageManager(sc);
    }

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    @BeforeClass
    public static void startCassandra() {
        ch.startCassandra();
    }

    @AfterClass
    public static void stopCassandra() {
        ch.stopCassandra();
    }
}
