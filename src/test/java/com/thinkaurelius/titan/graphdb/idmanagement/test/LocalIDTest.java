package com.thinkaurelius.titan.graphdb.idmanagement.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class LocalIDTest {

	int bitoffset = 20;
	long maxID = 1l<<bitoffset;
	GraphDatabaseConfiguration config;
	ObjectDiskStorage objStore;
	String configFile = "localIDtest";
	
	@Before
	public void setUp() {
		DiskgraphTest.deleteHomeDir();
		config = new GraphDatabaseConfiguration(DiskgraphTest.homeDirFile);
		objStore = new ObjectDiskStorage(config);
	}
	
	@After
	public void tearDown() {
	}
	
	@Test
	public void testLocalID() {
		constructLocalID(4,2,5);
		constructLocalID(5,3,5);
		constructLocalID(4,2,9);
		constructLocalID(5,4,14);
	}
	

	public void constructLocalID(int inserterOff, int inserterMinus, int inserter) {
		setUp();
		long maxNoInserter = (1<<inserterOff)-inserterMinus;
		IDPool id = new LocalID(maxID,maxNoInserter,inserter,objStore,configFile);
		int max = 1<<(bitoffset-inserterOff);
		for (int i=1;i<max;i++) {
			assertTrue(id.hasNext());
			assertEquals(id.nextID(),(i<<inserterOff) + inserter);
		}
		assertFalse(id.hasNext());
	}
	
	@Test
	public void testLocalIDRecall() {
		IDPool id = new LocalID(maxID,1,0,objStore,configFile);
		
		for (int i=1;i<1000;i++) {
			assertEquals(i,id.nextID());
		}
		id.close();
		
		id = new LocalID(maxID,1,0,objStore,configFile);
		for (int i=1000;i<10000;i++) {
			assertEquals(i,id.nextID());
			assertEquals(i,id.getCurrentID());
		}
		id.close();
	}
	
	
}
