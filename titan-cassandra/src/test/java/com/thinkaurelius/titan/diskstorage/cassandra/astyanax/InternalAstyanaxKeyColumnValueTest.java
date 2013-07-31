package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;

public class InternalAstyanaxKeyColumnValueTest extends AbstractCassandraKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager() throws StorageException {
        return new AstyanaxStoreManager(CassandraStorageSetup.getGenericCassandraStorageConfiguration(getClass().getSimpleName()));
    }
}
