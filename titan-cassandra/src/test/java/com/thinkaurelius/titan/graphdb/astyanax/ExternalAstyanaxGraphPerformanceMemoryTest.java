package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalAstyanaxGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    public ExternalAstyanaxGraphPerformanceMemoryTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration(ExternalAstyanaxGraphPerformanceMemoryTest.class.getSimpleName()));
    }

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    @BeforeClass
    public static void startCassandra() {
        ch.startCassandra();
    }

    @AfterClass
    public static void stopCassandra() {
        ch.stopCassandra();
    }

}
