package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class ExternalHBaseGraphPerformanceTest extends TitanGraphPerformanceTest {

	public ExternalHBaseGraphPerformanceTest() {
		super(StorageSetup.getHBaseGraphConfiguration(),0,1,false);
	}

    

}
