package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.IDAuthorityTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.BeforeClass;

public class AstyanaxIDAuthorityTest extends IDAuthorityTest {

    public AstyanaxIDAuthorityTest(WriteConfiguration baseConfig) {
        super(baseConfig);
    }

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new AstyanaxStoreManager(CassandraStorageSetup.getAstyanaxConfiguration(getClass().getSimpleName()));
    }
}
