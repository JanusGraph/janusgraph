package com.thinkaurelius.titan.graphdb.astyanax;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class ExternalAstyanaxGraphConcurrentTest extends TitanGraphConcurrentTest {

    public ExternalAstyanaxGraphConcurrentTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration(ExternalAstyanaxGraphConcurrentTest.class.getSimpleName()));
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
