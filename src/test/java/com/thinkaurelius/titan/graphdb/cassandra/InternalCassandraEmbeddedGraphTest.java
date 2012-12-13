package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class InternalCassandraEmbeddedGraphTest extends TitanGraphTest {

	public InternalCassandraEmbeddedGraphTest() {
		super(StorageSetup.getEmbeddedCassandraPartitionGraphConfiguration());
	}
}
