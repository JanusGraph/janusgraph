package com.thinkaurelius.titan.graphdb.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class StandardGraphDBPerformance extends AbstractGraphDBPerformance {

	public StandardGraphDBPerformance() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir),0,1,false);
	}
}
