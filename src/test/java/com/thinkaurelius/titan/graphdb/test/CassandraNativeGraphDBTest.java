package com.thinkaurelius.titan.graphdb.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.configuration.CassandraNativeStorageConfiguration;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.diskstorage.test.CassandraNativeLocalhostHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CassandraNativeGraphDBTest extends AbstractGraphDBTest {

	public static final String keyspace = "titantest00";
	
	public static final CassandraNativeLocalhostHelper ch =
		new CassandraNativeLocalhostHelper();
	
	private final CassandraNativeStorageConfiguration cassConf;
	
	@BeforeClass
	public static void beforeClass() {
		ch.mangleSystemProperties();
	}
	
	@AfterClass
	public static void afterClass() {
		ch.restoreSystemProperties();
	}
	
	public CassandraNativeGraphDBTest() {
		super(new GraphDatabaseConfiguration(DiskgraphTest.homeDir));
		cassConf = new CassandraNativeStorageConfiguration();
		cassConf.setKeyspace(keyspace);
		config.setStorage(cassConf);
	}
	
	@Before
	public void setUp() throws Exception {
		cassConf.dropAllStorage();
		super.setUp();
	}
	
	@Override
	public void open() {
		graphdb = config.openDatabase();
		tx=graphdb.startTransaction();
	}
}
