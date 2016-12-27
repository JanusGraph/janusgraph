package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;

public class AstyanaxStoreTest extends AbstractCassandraStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public ModifiableConfiguration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getAstyanaxConfiguration(getClass().getSimpleName());
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager(Configuration c) throws BackendException {
        return new AstyanaxStoreManager(c);
    }
}
