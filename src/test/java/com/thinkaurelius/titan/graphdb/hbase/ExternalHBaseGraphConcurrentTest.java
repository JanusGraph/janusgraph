package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseHelper;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalHBaseGraphConcurrentTest extends TitanGraphConcurrentTest {

	public ExternalHBaseGraphConcurrentTest() {
		super(StorageSetup.getHBaseGraphConfiguration());
	}

    @Override
    public void cleanUp() {
        HBaseHelper.deleteAll(StorageSetup.getHBaseGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
    }

    

}
