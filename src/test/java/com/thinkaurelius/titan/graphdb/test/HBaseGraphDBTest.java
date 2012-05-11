package com.thinkaurelius.titan.graphdb.test;

import com.thinkaurelius.titan.diskstorage.hbase.HBaseStorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Before;

import com.thinkaurelius.titan.DiskgraphTest;

public class HBaseGraphDBTest extends AbstractGraphDBTest  {
	
	private Configuration hbaseConf;
	
	public HBaseGraphDBTest() {
		super(getHBaseConfiguration());
	}
	
	@Before
	public void setUp() throws Exception {
		new HBaseStorageManager(getHBaseConfiguration()).deleteAll();
		super.setUp();
	}
    
    public static Configuration getHBaseConfiguration() {
        Configuration config = DiskgraphTest.getDefaultConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"hbase");
        return config;
    }
}
