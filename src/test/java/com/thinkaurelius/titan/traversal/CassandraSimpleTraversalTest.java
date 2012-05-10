package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.diskstorage.test.CassandraLocalhostHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CassandraSimpleTraversalTest extends AbstractSimpleTraversalTest {

	public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper();
	
	public CassandraSimpleTraversalTest() {
		super(ch.getConfiguration());
	}

	@BeforeClass
	public static void beforeClass() {
		ch.startCassandra();
	}
	
	@AfterClass
	public static void afterClass() throws InterruptedException {
		ch.stopCassandra();
	}
	
	@Before
	public void setUp() throws Exception {
		CassandraThriftStorageManager.dropKeyspace(
				CassandraStorageConfiguration.DEFAULT_KEYSPACE, 
				"127.0.0.1",
				CassandraStorageConfiguration.DEFAULT_PORT);
		super.setUp();
	}
}
