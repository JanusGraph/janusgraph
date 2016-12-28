package org.janusgraph.graphdb.astyanax;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.junit.BeforeClass;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.graphdb.TitanGraphPerformanceMemoryTest;

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
