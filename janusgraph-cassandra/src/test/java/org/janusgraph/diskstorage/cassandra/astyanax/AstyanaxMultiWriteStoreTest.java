package org.janusgraph.diskstorage.cassandra.astyanax;

import org.janusgraph.diskstorage.BackendException;
import org.junit.BeforeClass;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.MultiWriteKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class AstyanaxMultiWriteStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new AstyanaxStoreManager(CassandraStorageSetup.getAstyanaxConfiguration(getClass().getSimpleName()));
    }
}
