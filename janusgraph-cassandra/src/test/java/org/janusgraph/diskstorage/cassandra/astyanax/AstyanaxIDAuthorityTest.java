package org.janusgraph.diskstorage.cassandra.astyanax;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.IDAuthorityTest;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.BeforeClass;

public class AstyanaxIDAuthorityTest extends IDAuthorityTest {

    public AstyanaxIDAuthorityTest(WriteConfiguration baseConfig) {
        super(baseConfig);
    }

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new AstyanaxStoreManager(CassandraStorageSetup.getAstyanaxConfiguration(getClass().getSimpleName()));
    }
}
