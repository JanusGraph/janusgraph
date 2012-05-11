package com.thinkaurelius.titan.graphdb.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class StandardGraphDBPerformance extends AbstractGraphDBPerformance {

	public StandardGraphDBPerformance() {
		super(DiskgraphTest.getDefaultConfiguration(),0,1,false);
	}
}
