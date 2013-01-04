package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalCassandraGraphTest extends TitanGraphTest {

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    public ExternalCassandraGraphTest() {
        super(StorageSetup.getCassandraThriftGraphConfiguration());
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
