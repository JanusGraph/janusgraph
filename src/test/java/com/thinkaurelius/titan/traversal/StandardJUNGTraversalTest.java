package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;

public class StandardJUNGTraversalTest extends AbstractJUNGTraversalTest {

	public StandardJUNGTraversalTest() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir));
	}	
}

