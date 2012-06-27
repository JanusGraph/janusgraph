package com.thinkaurelius.titan.graphdb.astyanax;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.astyanax.AstyanaxStorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraDaemonWrapper;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.testutil.CassandraUtil;

public class InternalAstyanaxGraphTest extends TitanGraphTest {

	@BeforeClass
	public static void startCassandra() {
    	CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
	}
	
	public InternalAstyanaxGraphTest() {
		super(StorageSetup.getAstyanaxGraphConfiguration());
	}

}
