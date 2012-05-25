package com.thinkaurelius.titan.diskstorage.test;

import org.junit.After;
import org.junit.Before;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction;

public class CassandraLockKeyColumnValueStoreTest 
	extends LockKeyColumnValueStoreTest {

	public static final String keyspace = "titantest00";
	public static final String columnFamily = "test";

	public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper("127.0.0.1");
	
	@Override
	public void open() {
	}
	
	@Override
	public void close() {
	}
	
	@Before
	public void cassandraSetUp() {
		ch.startCassandra();
        CassandraThriftStorageManager cmanager = new CassandraThriftStorageManager(CassandraLocalhostHelper.getLocalStorageConfiguration());
        cmanager.dropKeyspace(keyspace);
        manager = cmanager;
		store = manager.openDatabase(columnFamily);
		host1tx1 = manager.beginTransaction();
		host1tx2 = manager.beginTransaction();
		host2tx1 = manager.beginTransaction();
		
//		((CassandraTransaction)host1tx1).setRid(rid1);
//		((CassandraTransaction)host1tx2).setRid(rid1);
//		((CassandraTransaction)host2tx1).setRid(rid2);
//
//		((CassandraTransaction)host1tx1).setLocalLockMediatorProvider(p1);
//		((CassandraTransaction)host1tx2).setLocalLockMediatorProvider(p1);
//		((CassandraTransaction)host2tx1).setLocalLockMediatorProvider(p2);
		
	}
	
	@After
	public void cassandraTearDown() {
		store.close();
		manager.close();
		ch.stopCassandra();
	}
	

}
