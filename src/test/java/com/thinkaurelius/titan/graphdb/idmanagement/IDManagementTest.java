package com.thinkaurelius.titan.graphdb.idmanagement;


import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Random;

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
        testEntityID(0,6,242342     ,0      ,12);
        testEntityID(0,6,242342     ,0      ,1);
        try {
            testEntityID(0,6,242342     ,1      ,12);
            assertTrue(false);
        } catch (IllegalArgumentException e) {};
        try {
            testEntityID(0,6,242342     ,0      ,63);
            assertTrue(false);
        } catch (IllegalArgumentException e) {};

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
        assertEquals(group,eid.getGroupID(id));
		
		id = eid.getRelationshipTypeID(count, group, partition);
		assertTrue(eid.isRelationshipTypeID(id));
		assertTrue(eid.isEdgeTypeID(id));
        assertEquals(group, eid.getGroupID(id));
		assertEquals(eid.getPartitionID(id),partition);
	}
	
	@Test
	public void edgeTypeIDTest() {
		int partitionBits = 21, groupBits = 6;
		IDManager eid = new IDManager(partitionBits,groupBits);
		int trails = 1000000;
		assertEquals(eid.getMaxGroupID(),(1<<groupBits)-2);
		assertEquals(eid.getMaxPartitionID(),(1<<partitionBits)-1);
		
		for (int t=0;t<trails;t++) {
			long id;
			long groupID = RandomGenerator.randomLong(1, eid.getMaxGroupID());
            long partitionID = RandomGenerator.randomLong(1, eid.getMaxPartitionID());
			boolean isProperty=false;
			if (Math.random()<0.5) {
				id = eid.getRelationshipTypeID(RandomGenerator.randomLong(1, eid.getMaxEdgeTypeID()),
						groupID, partitionID);
				assertTrue(eid.isRelationshipTypeID(id));
                
			} else {
				isProperty=true;
				id = eid.getPropertyTypeID(RandomGenerator.randomLong(1, eid.getMaxEdgeTypeID()),
						groupID, partitionID);
				assertTrue(eid.isPropertyTypeID(id));

			}
            assertEquals(groupID,eid.getGroupID(id));
            assertEquals(partitionID,eid.getPartitionID(id));
   			assertTrue(eid.isEdgeTypeID(id));

            long nogroup = eid.removeGroupID(id);
            assertTrue(nogroup>=0);
            assertEquals(0,eid.getGroupID(nogroup));
            assertEquals(id,eid.addGroupID(nogroup,groupID));

            int dir = RandomGenerator.randomInt(0, 4);
            ByteBuffer b = IDHandler.getEdgeType(id,dir,eid);
            assertEquals(dir,IDHandler.getDirectionID(b.get(0)));
            assertEquals(id,IDHandler.readEdgeType(b,eid));

            ByteBuffer g = IDHandler.getEdgeTypeGroup(groupID,dir,eid);
            assertEquals(dir,IDHandler.getDirectionID(g.get(0)));
            assertEquals(1,g.limit());
            int group = g.get(0);
            if (group<0) group+=256;
            assertEquals(groupID,group%64);

		}


        KryoSerializer serializer = new KryoSerializer(true);
        for (int dir = 0; dir<4; dir++) {
            for (int t=0;t<1000;t++) {
                long etid = RandomGenerator.randomLong(1,eid.getMaxEdgeTypeID());
                ByteBuffer b = IDHandler.getEdgeType(etid,dir,eid);
                DataOutput out = serializer.getDataOutput(100,true);
                IDHandler.writeEdgeType(out,etid,dir,eid);
                assertEquals(b,out.getByteBuffer());

            }
        }
	}

    @Test
    public void keyTest() {
        Random random = new Random();
        for (int t=0;t<1000000;t++) {
            long i = Math.abs(random.nextLong());
            assertEquals(i, IDHandler.getKeyID(IDHandler.getKey(i)));
        }
        assertEquals(Long.MAX_VALUE,IDHandler.getKeyID(IDHandler.getKey(Long.MAX_VALUE)));
    }

    //@Test
    public void testBinaryFormat() {
        IDManager eid = new IDManager(0,6);
        long id = eid.getRelationshipTypeID(15,13,0);
        printBinary(id);
        printBinary(eid.removeGroupID(id));
    }
	
	public void printBinary(long id) {
		String s = Long.toBinaryString(id);
		while (s.length()<64) {
			s = "0"+s;
		}
		log.debug(s);
	}

}
