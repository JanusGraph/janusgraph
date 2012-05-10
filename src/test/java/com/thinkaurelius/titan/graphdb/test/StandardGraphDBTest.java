package com.thinkaurelius.titan.graphdb.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class StandardGraphDBTest extends AbstractGraphDBTest {

	public StandardGraphDBTest() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir));
	}
}
