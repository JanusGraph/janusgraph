package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;

public class StandardSimpleTraversalTest extends AbstractSimpleTraversalTest {

	public StandardSimpleTraversalTest() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir));
	}
}
