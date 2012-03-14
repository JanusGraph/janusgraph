package com.thinkaurelius.titan.graphdb.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;

public class StandardConcurrentGraphDBTest extends AbstractConcurrentGraphDBTest {

	public StandardConcurrentGraphDBTest() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir));
	}
}
