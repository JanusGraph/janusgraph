package com.thinkaurelius.titan.graphdb.cassandra;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraDaemonWrapper;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class InternalCassandraEmbeddedGraphTest extends TitanGraphTest {

	public InternalCassandraEmbeddedGraphTest() {
		super(StorageSetup.getEmbeddedCassandraGraphConfiguration());
	}

	@BeforeClass
	public static void beforeClass() {
		CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
	}
}