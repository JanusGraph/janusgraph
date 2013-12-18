package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;

public class InternalAstyanaxGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getAstyanaxGraphConfiguration(getClass().getSimpleName());
    }

}
