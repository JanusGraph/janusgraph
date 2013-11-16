package com.thinkaurelius.titan.graphdb.astyanax;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;

public class InternalAstyanaxGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    public InternalAstyanaxGraphPerformanceMemoryTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration(InternalAstyanaxGraphPerformanceMemoryTest.class.getSimpleName()));
    }

}
