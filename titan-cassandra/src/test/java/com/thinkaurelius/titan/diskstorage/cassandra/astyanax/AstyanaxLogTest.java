package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.thinkaurelius.titan.diskstorage.BackendException;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.log.KCVSLogTest;
import com.thinkaurelius.titan.testcategory.SerialTests;

@Category(SerialTests.class)
public class AstyanaxLogTest extends KCVSLogTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new AstyanaxStoreManager(CassandraStorageSetup.getAstyanaxConfiguration(getClass().getSimpleName()));
    }

}
