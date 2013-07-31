package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalCassandraGraphPerformanceTest extends TitanGraphPerformanceTest {


    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    public ExternalCassandraGraphPerformanceTest() {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(ExternalCassandraGraphPerformanceTest.class.getSimpleName()), 0, 1, false);
    }

    @BeforeClass
    public static void beforeClass() {
        ch.startCassandra();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        ch.stopCassandra();
    }

}
