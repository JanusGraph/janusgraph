package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalAstyanaxGraphPerformanceTest extends TitanGraphPerformanceTest {

    public ExternalAstyanaxGraphPerformanceTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration(ExternalAstyanaxGraphPerformanceTest.class.getSimpleName()), 0, 1, false);
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
