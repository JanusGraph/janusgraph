package com.thinkaurelius.titan.diskstorage.astyanax;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.testutil.CassandraUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalAstyanaxKeyColumnValueTest extends KeyColumnValueStoreTest {

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    @Override
    public StorageManager openStorageManager() {
        return new AstyanaxStorageManager(StorageSetup.getCassandraStorageConfiguration());
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
