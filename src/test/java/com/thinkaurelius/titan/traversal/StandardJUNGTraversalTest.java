package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class StandardJUNGTraversalTest extends AbstractJUNGTraversalTest {

	public StandardJUNGTraversalTest() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir));
	}	
}

