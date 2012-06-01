package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseHelper;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalHBaseGraphPerformanceTest extends TitanGraphPerformanceTest {

	public ExternalHBaseGraphPerformanceTest() {
		super(StorageSetup.getHBaseGraphConfiguration(),0,1,false);
	}

    @Override
    public void cleanUp() {
        HBaseHelper.deleteAll(StorageSetup.getHBaseGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
    }

    

}
