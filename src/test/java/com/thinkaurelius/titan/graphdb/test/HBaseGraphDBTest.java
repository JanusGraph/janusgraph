package com.thinkaurelius.titan.graphdb.test;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Before;

import com.thinkaurelius.titan.DiskgraphTest;

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
