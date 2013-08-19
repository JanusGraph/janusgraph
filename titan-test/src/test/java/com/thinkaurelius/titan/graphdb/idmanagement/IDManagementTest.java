package com.thinkaurelius.titan.graphdb.idmanagement;


import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
            RelationType type;
            if (Math.random() < 0.5) {
                id = eid.getEdgeLabelID(count);
                assertTrue(eid.isEdgeLabelID(id));
                type = RelationType.EDGE;
                if (Math.random() < 0.5)
                    dirID = IDHandler.EDGE_IN_DIR;
                else
                    dirID = IDHandler.EDGE_OUT_DIR;
            } else {
                type = RelationType.PROPERTY;
                id = eid.getPropertyKeyID(count);
                assertTrue(eid.isPropertyKeyID(id));
                dirID = IDHandler.PROPERTY_DIR;
            }
            assertTrue(eid.isTypeID(id));

            StaticBuffer b = IDHandler.getEdgeType(id, dirID);
//            System.out.println(dirID);
//            System.out.println(getBinary(id));
//            System.out.println(getBuffer(b.asReadBuffer()));
            ReadBuffer rb = b.asReadBuffer();
            long[] vals = IDHandler.readEdgeType(rb);
            assertEquals(id,vals[0]);
            assertEquals(dirID, vals[1]);
            assertFalse(rb.hasRemaining());

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
            StaticBuffer[] bounds = IDHandler.getBounds(type);
            assertTrue(bounds[0].compareTo(b)<0);
            assertTrue(bounds[1].compareTo(b)>0);
            bounds = IDHandler.getBounds(RelationType.RELATION);
            assertTrue(bounds[0].compareTo(b)<0);
            assertTrue(bounds[1].compareTo(b)>0);
        }
    }

    @Test
    public void testDirectionPrefix() {
        for (RelationType type : RelationType.values()) {
            StaticBuffer[] bounds = IDHandler.getBounds(type);
            assertEquals(1,bounds[0].length());
            assertEquals(1,bounds[1].length());
            assertTrue(bounds[0].compareTo(bounds[1])<0);
        }
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
