package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalCassandraGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {


    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    public ExternalCassandraGraphPerformanceMemoryTest() {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(ExternalCassandraGraphPerformanceMemoryTest.class.getSimpleName()));
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
