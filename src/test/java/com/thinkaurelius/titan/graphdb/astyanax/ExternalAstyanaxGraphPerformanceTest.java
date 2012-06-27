package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.astyanax.AstyanaxStorageManager;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;
import com.thinkaurelius.titan.testutil.CassandraUtil;

public class ExternalAstyanaxGraphPerformanceTest extends TitanGraphPerformanceTest {
	
	public ExternalAstyanaxGraphPerformanceTest() {
		super(StorageSetup.getAstyanaxGraphConfiguration(), 0, 1, false);
	}

}
