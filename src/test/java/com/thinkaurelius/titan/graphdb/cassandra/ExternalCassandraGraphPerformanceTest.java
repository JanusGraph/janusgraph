package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class ExternalCassandraGraphPerformanceTest extends TitanGraphPerformanceTest {


    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    public ExternalCassandraGraphPerformanceTest() {
        super(StorageSetup.getCassandraGraphConfiguration(),0,1,false);
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
