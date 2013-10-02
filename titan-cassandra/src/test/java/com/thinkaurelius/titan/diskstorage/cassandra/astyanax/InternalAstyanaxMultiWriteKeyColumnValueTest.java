package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class InternalAstyanaxMultiWriteKeyColumnValueTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = CassandraStorageSetup.getGenericCassandraStorageConfiguration(getClass().getSimpleName());
        return new AstyanaxStoreManager(config);
    }
}
