package com.thinkaurelius.titan.graphdb.idmanagement;


import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.PartitionIDRange;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.types.system.*;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class IDManagementTest {

    private static final Logger log = LoggerFactory.getLogger(IDManagementTest.class);

    private static final Random random = new Random();

    private static final IDManager.VertexIDType[] USER_VERTEX_TYPES = {IDManager.VertexIDType.NormalVertex,
            IDManager.VertexIDType.PartitionedVertex, IDManager.VertexIDType.UnmodifiableVertex};

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void EntityIDTest() {
        testEntityID(12, 2341, 1234123, 1235123);
        testEntityID(16, 64000, 582919, 583219);
        testEntityID(4, 14, 1, 1000);
        testEntityID(10, 1, 903392, 903592);
        testEntityID(0, 0, 242342, 249342);
        try {
            testEntityID(0, 1, 242342, 242345);
            fail();
        } catch (IllegalArgumentException e) {}

        try {
            testEntityID(0, 0, -11, -10);
            fail();
        } catch (IllegalArgumentException e) {}

    }


    public void testEntityID(int partitionBits, int partition, long minCount, long maxCount) {
        IDManager eid = new IDManager(partitionBits);
        IDInspector isp = eid.getIdInspector();

        assertTrue(eid.getPartitionBound()>0);
        assertTrue(eid.getPartitionBound()<=1l+Integer.MAX_VALUE);
        assertTrue(eid.getRelationCountBound()>0);
        assertTrue(eid.getSchemaCountBound()>0);
        assertTrue(eid.getVertexCountBound()>0);

        try {
            IDManager.getTemporaryVertexID(IDManager.VertexIDType.RelationType,5);
            fail();
        } catch (IllegalArgumentException e) {}

        for (long count=minCount;count<maxCount;count++) {

            for (IDManager.VertexIDType vtype : USER_VERTEX_TYPES) {
                if (partitionBits==0 && vtype== IDManager.VertexIDType.PartitionedVertex) continue;
                if (vtype== IDManager.VertexIDType.PartitionedVertex) partition=IDManager.PARTITIONED_VERTEX_PARTITION;
                long id = eid.getVertexID(count, partition,vtype);
                assertTrue(isp.isUserVertexId(id));
                assertTrue(vtype.is(id));
                if (vtype != IDManager.VertexIDType.PartitionedVertex) assertEquals(eid.getPartitionId(id), partition);
                assertEquals(id, eid.getKeyID(eid.getKey(id)));
            }

            long id = eid.getRelationID(count, partition);
            assertTrue(id>=partition);

            id = eid.getSchemaId(IDManager.VertexIDType.UserPropertyKey, count);
            assertTrue(isp.isPropertyKeyId(id));
            assertTrue(isp.isRelationTypeId(id));
            assertFalse(isp.isSystemRelationTypeId(id));

            assertEquals(id, eid.getKeyID(eid.getKey(id)));

            id = eid.getSchemaId(IDManager.VertexIDType.SystemPropertyKey, count);
            assertTrue(isp.isPropertyKeyId(id));
            assertTrue(isp.isRelationTypeId(id));
            assertTrue(isp.isSystemRelationTypeId(id));


            id = eid.getSchemaId(IDManager.VertexIDType.UserEdgeLabel,count);
            assertTrue(isp.isEdgeLabelId(id));
            assertTrue(isp.isRelationTypeId(id));

            assertEquals(id, eid.getKeyID(eid.getKey(id)));

            id = eid.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex,count);
            assertTrue(eid.isTemporary(id));
            assertTrue(IDManager.VertexIDType.NormalVertex.is(id));

            id = eid.getTemporaryVertexID(IDManager.VertexIDType.UserEdgeLabel,count);
            assertTrue(eid.isTemporary(id));
            assertTrue(IDManager.VertexIDType.UserEdgeLabel.is(id));

            id = IDManager.getTemporaryRelationID(count);
            assertTrue(eid.isTemporary(id));

            id = IDManager.getTemporaryVertexID(IDManager.VertexIDType.HiddenVertex,count);
            assertTrue(eid.isTemporary(id));
            assertTrue(IDManager.VertexIDType.Hidden.is(id));

        }
    }

    @Test
    public void edgeTypeIDTest() {
        int partitionBits = 16;
        IDManager eid = new IDManager(partitionBits);
        IDInspector isp = eid.getIdInspector();
        int trails = 1000000;
        assertEquals(eid.getPartitionBound(), (1l << partitionBits));

        Serializer serializer = new StandardSerializer();
        for (int t = 0; t < trails; t++) {
            long count = RandomGenerator.randomLong(1, eid.getSchemaCountBound());
            long id;
            IDHandler.DirectionID dirID;
            RelationCategory type;
            if (Math.random() < 0.5) {
                id = eid.getSchemaId(IDManager.VertexIDType.UserEdgeLabel,count);
                assertTrue(isp.isEdgeLabelId(id));
                assertFalse(isp.isSystemRelationTypeId(id));
                type = RelationCategory.EDGE;
                if (Math.random() < 0.5)
                    dirID = IDHandler.DirectionID.EDGE_IN_DIR;
                else
                    dirID = IDHandler.DirectionID.EDGE_OUT_DIR;
            } else {
                type = RelationCategory.PROPERTY;
                id = eid.getSchemaId(IDManager.VertexIDType.UserPropertyKey,count);
                assertTrue(isp.isPropertyKeyId(id));
                assertFalse(isp.isSystemRelationTypeId(id));
                dirID = IDHandler.DirectionID.PROPERTY_DIR;
            }
            assertTrue(isp.isRelationTypeId(id));

            StaticBuffer b = IDHandler.getEdgeType(id, dirID, false);
//            System.out.println(dirID);
//            System.out.println(getBinary(id));
//            System.out.println(getBuffer(b.asReadBuffer()));
            ReadBuffer rb = b.asReadBuffer();
            IDHandler.EdgeTypeParse parse = IDHandler.readEdgeType(rb);
            assertEquals(id,parse.typeId);
            assertEquals(dirID, parse.dirID);
            assertFalse(rb.hasRemaining());

            //Inline edge type
            WriteBuffer wb = new WriteByteBuffer(9);
            IDHandler.writeInlineEdgeType(wb, id);
            long newId = IDHandler.readInlineEdgeType(wb.getStaticBuffer().asReadBuffer());
            assertEquals(id,newId);

            //Compare to Kryo
            DataOutput out = serializer.getDataOutput(10);
            IDHandler.writeEdgeType(out, id, dirID, false);
            assertEquals(b, out.getStaticBuffer());

            //Make sure the bounds are right
            StaticBuffer[] bounds = IDHandler.getBounds(type,false);
            assertTrue(bounds[0].compareTo(b)<0);
            assertTrue(bounds[1].compareTo(b)>0);
            bounds = IDHandler.getBounds(RelationCategory.RELATION,false);
            assertTrue(bounds[0].compareTo(b)<0);
            assertTrue(bounds[1].compareTo(b)>0);
        }
    }

    private static final SystemRelationType[] SYSTEM_TYPES = {BaseKey.VertexExists, BaseKey.SchemaDefinitionProperty,
            BaseLabel.SchemaDefinitionEdge, ImplicitKey.VISIBILITY, ImplicitKey.TIMESTAMP};

    @Test
    public void writingInlineEdgeTypes() {
        int numTries = 100;
        WriteBuffer out = new WriteByteBuffer(8*numTries);
        for (SystemRelationType t : SYSTEM_TYPES) {
            IDHandler.writeInlineEdgeType(out,t.getID());
        }
        for (long i=1;i<=numTries;i++) {
            IDHandler.writeInlineEdgeType(out, IDManager.getSchemaId(IDManager.VertexIDType.UserEdgeLabel, i * 1000));
        }

        ReadBuffer in = out.getStaticBuffer().asReadBuffer();
        for (SystemRelationType t : SYSTEM_TYPES) {
            assertEquals(t, SystemTypeManager.getSystemType(IDHandler.readInlineEdgeType(in)));
        }
        for (long i=1;i<=numTries;i++) {
            assertEquals(i * 1000, IDManager.stripEntireRelationTypePadding(IDHandler.readInlineEdgeType(in)));
        }
    }

    @Test
    public void testDirectionPrefix() {
        for (RelationCategory type : RelationCategory.values()) {
            for (boolean system : new boolean[]{true,false}) {
                StaticBuffer[] bounds = IDHandler.getBounds(type,system);
                assertEquals(1,bounds[0].length());
                assertEquals(1,bounds[1].length());
                assertTrue(bounds[0].compareTo(bounds[1])<0);
                assertTrue(bounds[1].compareTo(BufferUtil.oneBuffer(1))<0);
            }
        }
    }

    @Test
    public void testEdgeTypeWriting() {
        for (SystemRelationType t : SYSTEM_TYPES) {
            testEdgeTypeWriting(t.getID());
        }
        for (int i=0;i<1000;i++) {
            IDManager.VertexIDType type = random.nextDouble()<0.5? IDManager.VertexIDType.UserPropertyKey: IDManager.VertexIDType.UserEdgeLabel;
            testEdgeTypeWriting(IDManager.getSchemaId(type,random.nextInt(1000000000)));
        }
    }

    public void testEdgeTypeWriting(long etid) {
        IDHandler.DirectionID[] dir = IDManager.VertexIDType.EdgeLabel.is(etid)?
                    new IDHandler.DirectionID[]{IDHandler.DirectionID.EDGE_IN_DIR, IDHandler.DirectionID.EDGE_OUT_DIR}:
                    new IDHandler.DirectionID[]{IDHandler.DirectionID.PROPERTY_DIR};
        RelationCategory relCat = IDManager.VertexIDType.EdgeLabel.is(etid)?RelationCategory.EDGE:RelationCategory.PROPERTY;
        boolean hidden = IDManager.isSystemRelationTypeId(etid);
        for (IDHandler.DirectionID d : dir) {
            StaticBuffer b = IDHandler.getEdgeType(etid,d,hidden);
            IDHandler.EdgeTypeParse parse = IDHandler.readEdgeType(b.asReadBuffer());
            assertEquals(d,parse.dirID);
            assertEquals(etid,parse.typeId);
        }
    }

    @Test
    public void testUserVertexBitWdith() {
        for (IDManager.VertexIDType type : IDManager.VertexIDType.values()) {
            if (IDManager.VertexIDType.UserVertex.is(type.suffix()) && type.isProper())
                assert type.offset()==IDManager.USERVERTEX_PADDING_BITWIDTH;
            assertTrue(type.offset()<=IDManager.MAX_PADDING_BITWIDTH);
        }
    }

    @Test
    public void partitionIDRangeTest() {

        List<PartitionIDRange> result = PartitionIDRange.getIDRanges(16, ImmutableList.of(getKeyRange(120<<16, 6, 140<<16, 8)));
        assertTrue(result.size()==1);
        PartitionIDRange r = result.get(0);
        assertEquals(121,r.getLowerID());
        assertEquals(140,r.getUpperID());
        assertEquals(1<<16,r.getIdUpperBound());

        result = PartitionIDRange.getIDRanges(16, ImmutableList.of(getKeyRange(120<<16, 0, 140<<16, 0)));
        assertTrue(result.size()==1);
        r = result.get(0);
        assertEquals(120,r.getLowerID());
        assertEquals(140,r.getUpperID());

        result = PartitionIDRange.getIDRanges(8, ImmutableList.of(getKeyRange(250<<24, 0, 0<<24, 0)));
        assertTrue(result.size()==1);
        r = result.get(0);
        assertEquals(250,r.getLowerID());
        assertEquals(0,r.getUpperID());

        for (int i=0;i<255;i=i+5) {
            result = PartitionIDRange.getIDRanges(8, ImmutableList.of(getKeyRange(i<<24, 0, i<<24, 0)));
            assertTrue(result.size()==1);
            r = result.get(0);
            for (int j=0;j<255;j++) assertTrue(r.contains(j));
        }

        result = PartitionIDRange.getIDRanges(8, ImmutableList.of(getKeyRange(1<<24, 0, 1<<24, 1)));
        assertTrue(result.isEmpty());

        result = PartitionIDRange.getIDRanges(8, ImmutableList.of(getKeyRange(1<<28, 6, 1<<28, 8)));
        assertTrue(result.isEmpty());

        result = PartitionIDRange.getIDRanges(8, ImmutableList.of(getKeyRange(33<<24, 6, 34<<24, 8)));
        assertTrue(result.isEmpty());

    }

    private static KeyRange getKeyRange(int s1, long l1, int s2, long l2) {
        return new KeyRange(getBufferOf(s1,l1),getBufferOf(s2,l2));

    }

    private static StaticBuffer getBufferOf(int s, long l) {
        WriteBuffer out = new WriteByteBuffer(4+8);
        out.putInt(s);
        out.putLong(l);
        return out.getStaticBuffer();
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
