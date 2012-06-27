package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.astyanax.AstyanaxStorageManager;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.testutil.CassandraUtil;

public class ExternalAstyanaxGraphTest extends TitanGraphTest {

	public ExternalAstyanaxGraphTest() {
		super(StorageSetup.getAstyanaxGraphConfiguration());
	}
}
