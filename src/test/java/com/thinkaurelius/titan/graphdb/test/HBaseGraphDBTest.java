package com.thinkaurelius.titan.graphdb.test;

import org.junit.Before;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.configuration.HBaseStorageConfiguration;

public class HBaseGraphDBTest extends AbstractGraphDBTest  {
	
	private HBaseStorageConfiguration hbaseConf; 
	
	public HBaseGraphDBTest() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir));
		hbaseConf = new HBaseStorageConfiguration();
		config.setStorage(hbaseConf);
	}
	
	@Before
	public void setUp() throws Exception {
		hbaseConf.deleteAll();
		super.setUp();
	}
}
