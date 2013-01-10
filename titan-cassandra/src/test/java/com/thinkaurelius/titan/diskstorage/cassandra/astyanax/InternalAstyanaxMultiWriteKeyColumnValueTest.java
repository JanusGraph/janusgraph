package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.BeforeClass;

public class InternalAstyanaxMultiWriteKeyColumnValueTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new AstyanaxStoreManager(CassandraStorageSetup.getCassandraStorageConfiguration());
    }
}
