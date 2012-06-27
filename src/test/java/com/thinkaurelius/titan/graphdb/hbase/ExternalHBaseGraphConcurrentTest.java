package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class ExternalHBaseGraphConcurrentTest extends TitanGraphConcurrentTest {

	public ExternalHBaseGraphConcurrentTest() {
		super(StorageSetup.getHBaseGraphConfiguration());
	}

    

}
