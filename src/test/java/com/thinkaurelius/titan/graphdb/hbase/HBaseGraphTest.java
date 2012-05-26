package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.diskstorage.hbase.HBaseHelper;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Before;

import com.thinkaurelius.titan.StorageSetup;

public class HBaseGraphTest extends TitanGraphTest {

	public HBaseGraphTest() {
		super(StorageSetup.getHBaseGraphConfiguration());
	}

    @Override
    public void cleanUp() {
        HBaseHelper.deleteAll(StorageSetup.getHBaseGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
    }

    

}
