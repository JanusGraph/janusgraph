package com.thinkaurelius.titan.graphdb.idmanagement.test;


import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.util.test.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IDManagementTest {

	private static final Logger log = LoggerFactory.getLogger(IDManagementTest.class);
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void EntityIDTest() {
		testEntityID(21,7 ,1234123	,2341	,34);
		testEntityID(25,10,582919	,9921239,233);
		testEntityID(4 ,3 ,1		,14		,6);
		testEntityID(10,10,903392	,1		,1);
	}
	
	public void testEntityID(int partitionBits, int groupBits, long count, int partition, int group) {
		IDManager eid = new IDManager(partitionBits,groupBits);
		
		long id = eid.getNodeID(count, partition);
		assertTrue(eid.isNodeID(id));
		assertEquals(eid.getPartitionID(id),partition);
		
		id = eid.getEdgeID(count);
		assertTrue(eid.isEdgeID(id));

		id = eid.getPropertyTypeID(count, group, partition);
		assertTrue(eid.isPropertyTypeID(id));
		assertTrue(eid.isEdgeTypeID(id));
		assertEquals(eid.getPartitionID(id),partition);
		
		id = eid.getRelationshipTypeID(count, group, partition);
		assertTrue(eid.isRelationshipTypeID(id));
		assertTrue(eid.isEdgeTypeID(id));
		assertEquals(eid.getPartitionID(id),partition);
	}
	
	@Test
	public void edgeTypeIDTest() {
		int partitionBits = 21, groupBits = 5;
		IDManager eid = new IDManager(partitionBits,groupBits);
		int trails = 1000000;
		assertEquals(eid.getMaxGroupID(),(1<<groupBits)-2);
		assertEquals(eid.getMaxPartitionID(),(1<<partitionBits)-1);
		
		for (int t=0;t<trails;t++) {
			long id;
			long groupID = RandomGenerator.randomLong(1, eid.getMaxGroupID());
			boolean isProperty=false;
			long padding;
			long dir = RandomGenerator.randomInt(0, 2);
			if (Math.random()<0.5) {
				id = eid.getRelationshipTypeID(RandomGenerator.randomLong(1, eid.getMaxEdgeTypeID()),
						groupID,
						RandomGenerator.randomLong(1, eid.getMaxPartitionID()));
				assertTrue(eid.isRelationshipTypeID(id));
				padding = IDManager.IDType.RelationshipType.id();
				
				assertEquals(((dir<<3)+padding)<<58,eid.getQueryBoundsRelationship(dir)[0]);
				assertEquals(((dir<<3)+padding+1)<<58,eid.getQueryBoundsRelationship(dir)[1]);
			} else {
				isProperty=true;
				id = eid.getPropertyTypeID(RandomGenerator.randomLong(1, eid.getMaxEdgeTypeID()),
						groupID,
						RandomGenerator.randomLong(1, eid.getMaxPartitionID()));
				assertTrue(eid.isPropertyTypeID(id));
				padding = IDManager.IDType.PropertyType.id();
				
				assertEquals(((dir<<3)+padding)<<58,eid.getQueryBoundsProperty(dir)[0]);
				assertEquals(((dir<<3)+padding+1)<<58,eid.getQueryBoundsProperty(dir)[1]);

			}
			assertTrue(eid.isEdgeTypeID(id));
			
			long moved = eid.switchEdgeTypeID(id, dir);
			assertTrue(isProperty ^ eid.isRelationshipTypeFront(moved));
			assertTrue(!(isProperty ^ eid.isPropertyTypeFront(moved)));
			assertEquals(groupID,eid.getGroupIDFront(moved));
			assertEquals(dir,eid.getDirectionFront(moved));
			assertEquals(id,eid.switchBackEdgeTypeID(moved));
			
			assertEquals(dir<<61,eid.getQueryBounds(dir)[0]);
			assertEquals((dir+1)<<61,eid.getQueryBounds(dir)[1]);
			
			
		}
		
	}
	
	public void printBinary(long id) {
		String s = Long.toBinaryString(id);
		while (s.length()<64) {
			s = "0"+s;
		}
		log.debug(s);
	}

}
