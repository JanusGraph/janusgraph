package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

public class InternalCassandraEmbeddedGraphTest extends TitanGraphTest {

	public InternalCassandraEmbeddedGraphTest() {
		super(StorageSetup.getEmbeddedCassandraPartitionGraphConfiguration());
	}

}
