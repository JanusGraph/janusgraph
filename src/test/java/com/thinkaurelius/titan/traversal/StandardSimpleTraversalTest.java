package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class StandardSimpleTraversalTest extends AbstractSimpleTraversalTest {

	public StandardSimpleTraversalTest() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir));
	}
}
