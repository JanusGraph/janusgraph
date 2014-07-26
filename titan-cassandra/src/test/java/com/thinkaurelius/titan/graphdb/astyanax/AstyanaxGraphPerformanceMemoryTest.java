package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;

public class AstyanaxGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getAstyanaxGraphConfiguration(getClass().getSimpleName());
    }

}
