package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalAstyanaxGraphTest extends TitanGraphTest {

	public ExternalAstyanaxGraphTest() {
		super(StorageSetup.getAstyanaxGraphConfiguration());
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
