package com.thinkaurelius.titan.graphdb.idmanagement;


import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        testEntityID(21, 1234123, 2341);
        testEntityID(25, 582919, 9921239);
        testEntityID(4, 1, 14);
        testEntityID(10, 903392, 1);
        testEntityID(0, 242342, 0);
        testEntityID(0, 242342, 0);
        try {
            testEntityID(0, 242342, 1);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
        }
        ;
        try {
            testEntityID(0, -11, 0);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
        }
        ;

    }


    public void testEntityID(int partitionBits, long count, int partition) {
        IDManager eid = new IDManager(partitionBits);

        long id = eid.getVertexID(count, partition);
        assertTrue(eid.isVertexID(id));
        assertEquals(eid.getPartitionID(id), partition);

        id = eid.getRelationID(count, partition);
        assertTrue(eid.isRelationID(id));
        assertEquals(eid.getPartitionID(id), partition);

        id = eid.getPropertyKeyID(count);
        assertTrue(eid.isPropertyKeyID(id));
        assertTrue(eid.isTypeID(id));

        id = eid.getEdgeLabelID(count);
        assertTrue(eid.isEdgeLabelID(id));
        assertTrue(eid.isTypeID(id));
    }

    @Test
    public void edgeTypeIDTest() {
        int partitionBits = 21;
        IDManager eid = new IDManager(partitionBits);
        int trails = 1000000;
        assertEquals(eid.getMaxPartitionCount(), (1 << partitionBits) - 1);

        KryoSerializer serializer = new KryoSerializer(true);
        for (int t = 0; t < trails; t++) {
            long count = RandomGenerator.randomLong(1, eid.getMaxTitanTypeCount());
            long id;
            int dirID;
            if (Math.random() < 0.5) {
                id = eid.getEdgeLabelID(count);
                assertTrue(eid.isEdgeLabelID(id));
                if (Math.random() < 0.5)
                    dirID = IDManager.EDGE_IN_DIR;
                else
                    dirID = IDManager.EDGE_OUT_DIR;
            } else {
                id = eid.getPropertyKeyID(count);
                assertTrue(eid.isPropertyKeyID(id));
                dirID = IDManager.PROPERTY_DIR;
            }
            assertTrue(eid.isTypeID(id));

            StaticBuffer b = IDHandler.getEdgeType(id, dirID);
//            System.out.println(dirID);
//            System.out.println(getBinary(id));
//            System.out.println(getBuffer(b.asReadBuffer()));
            long[] vals = IDHandler.readEdgeType(b.asReadBuffer());
            assertEquals(id,vals[0]);
            assertEquals(dirID, vals[1]);

            //Inline edge type
            WriteBuffer wb = new WriteByteBuffer(9);
            IDHandler.writeInlineEdgeType(wb, id);
            long newId = IDHandler.readInlineEdgeType(wb.getStaticBuffer().asReadBuffer());
            assertEquals(id,newId);

            //Compare to Kryo
            DataOutput out = serializer.getDataOutput(10, true);
            IDHandler.writeEdgeType(out, id, dirID);
            assertEquals(b, out.getStaticBuffer());

            //Make sure the bounds are right
            StaticBuffer lower = IDHandler.directionPlusZero(dirID);
            StaticBuffer upper = IDHandler.directionPlusOne(dirID);
            assertTrue(lower.compareTo(b)<0);
            assertTrue(upper.compareTo(b)>0);
        }
    }

    @Test
    public void testDirectionPrefix() {
        for (int dirID=0;dirID<4;dirID++) {
            if (IDHandler.isValidDirection(dirID)) {
                ReadBuffer rb = IDHandler.directionPlusOne(dirID).asReadBuffer();
//                System.out.println(getBuffer(IDHandler.directionPlusOne(dirID).asReadBuffer()));
                boolean first=true;
                while (rb.hasRemaining()) {
                    int b = VariableLong.unsignedByte(rb.getByte());
                    if (first) {
                        assertEquals("Original value: "+b,dirID,(b>>>6));
                        assertEquals(64-1 , b & ((1<<6)-1));
                        first = false;
                    } else {
                        assertEquals(255,b);
                    }
                }

                rb = IDHandler.directionPlusZero(dirID).asReadBuffer();
//                System.out.println(getBuffer(IDHandler.directionPlusZero(dirID).asReadBuffer()));
                first=true;
                while (rb.hasRemaining()) {
                    int b = VariableLong.unsignedByte(rb.getByte());
                    if (first) {
                        assertEquals(dirID,(b>>>6));
                        assertEquals(0 , b & ((1<<6)-1));
                        first = false;
                    } else {
                        assertEquals(0,b);
                    }
                }
            }
        }
    }

    public static final int getRandomDirectionDir() {
        return RandomGenerator.randomInt(0, 3);
    }

    @Test
    public void keyTest() {
        Random random = new Random();
        for (int t = 0; t < 1000000; t++) {
            long i = Math.abs(random.nextLong());
            assertEquals(i, IDHandler.getKeyID(IDHandler.getKey(i)));
        }
        assertEquals(Long.MAX_VALUE, IDHandler.getKeyID(IDHandler.getKey(Long.MAX_VALUE)));
    }

    //@Test
    public void testBinaryFormat() {
        IDManager eid = new IDManager(0);
        long id = eid.getEdgeLabelID(15);
        System.out.println(getBinary(id));
    }

    public String getBuffer(ReadBuffer r) {
        String result = "";
        while (r.hasRemaining()) {
            result += getBinary(VariableLong.unsignedByte(r.getByte()),8) + " ";
        }
        return result;
    }

    public String getBinary(long id) {
        return getBinary(id,64);
    }

    public String getBinary(long id, int normalizedLength) {
        String s = Long.toBinaryString(id);
        while (s.length() < normalizedLength) {
            s = "0" + s;
        }
        return s;
    }

}
