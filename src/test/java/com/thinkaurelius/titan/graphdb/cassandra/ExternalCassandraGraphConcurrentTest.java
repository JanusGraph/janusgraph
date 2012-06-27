package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class ExternalCassandraGraphConcurrentTest extends TitanGraphConcurrentTest {

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    public ExternalCassandraGraphConcurrentTest() {
        super(StorageSetup.getCassandraGraphConfiguration());
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