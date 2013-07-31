package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalCassandraGraphConcurrentTest extends TitanGraphConcurrentTest {

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    public ExternalCassandraGraphConcurrentTest() {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(ExternalCassandraGraphConcurrentTest.class.getSimpleName()));
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