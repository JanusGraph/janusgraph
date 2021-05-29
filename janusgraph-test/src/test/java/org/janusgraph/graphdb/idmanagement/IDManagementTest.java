// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.idmanagement;


import com.google.common.collect.ImmutableList;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.database.idassigner.placement.PartitionIDRange;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.graphdb.types.system.SystemRelationType;
import org.janusgraph.graphdb.types.system.SystemTypeManager;
import org.janusgraph.testutil.RandomGenerator;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class IDManagementTest {

    private static final Random random = new Random();

    private static final IDManager.VertexIDType[] USER_VERTEX_TYPES = {IDManager.VertexIDType.NormalVertex,
            IDManager.VertexIDType.PartitionedVertex, IDManager.VertexIDType.UnmodifiableVertex};

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
        } catch (IllegalArgumentException ignored) {}

        try {
            testEntityID(0, 0, -11, -10);
            fail();
        } catch (IllegalArgumentException ignored) {}

    }


    public void testEntityID(int partitionBits, int partition, long minCount, long maxCount) {
        IDManager eid = new IDManager(partitionBits);

        assertTrue(eid.getPartitionBound()>0);
        assertTrue(eid.getPartitionBound()<= 1L +Integer.MAX_VALUE);
        assertTrue(eid.getRelationCountBound()>0);
        assertTrue(IDManager.getSchemaCountBound()>0);
        assertTrue(eid.getVertexCountBound()>0);

        try {
            IDManager.getTemporaryVertexID(IDManager.VertexIDType.RelationType,5);
            fail();
        } catch (IllegalArgumentException ignored) {}

        for (long count=minCount;count<maxCount;count++) {

            for (IDManager.VertexIDType userVertexType : USER_VERTEX_TYPES) {
                if (partitionBits==0 && userVertexType== IDManager.VertexIDType.PartitionedVertex) continue;
                if (userVertexType== IDManager.VertexIDType.PartitionedVertex) partition=IDManager.PARTITIONED_VERTEX_PARTITION;
                long id = eid.getVertexID(count, partition,userVertexType);
                assertTrue(eid.isUserVertexId(id));
                assertTrue(userVertexType.is(id));
                if (userVertexType != IDManager.VertexIDType.PartitionedVertex) assertEquals(eid.getPartitionId(id), partition);
                assertEquals(id, eid.getKeyID(eid.getKey(id)));
            }

            long id = eid.getRelationID(count, partition);
            assertTrue(id>=partition);

            id = IDManager.getSchemaId(IDManager.VertexIDType.UserPropertyKey, count);
            assertTrue(eid.isPropertyKeyId(id));
            assertTrue(eid.isRelationTypeId(id));
            assertFalse(IDManager.isSystemRelationTypeId(id));

            assertEquals(id, eid.getKeyID(eid.getKey(id)));

            id = IDManager.getSchemaId(IDManager.VertexIDType.SystemPropertyKey, count);
            assertTrue(eid.isPropertyKeyId(id));
            assertTrue(eid.isRelationTypeId(id));
            assertTrue(IDManager.isSystemRelationTypeId(id));


            id = IDManager.getSchemaId(IDManager.VertexIDType.UserEdgeLabel,count);
            assertTrue(eid.isEdgeLabelId(id));
            assertTrue(eid.isRelationTypeId(id));

            assertEquals(id, eid.getKeyID(eid.getKey(id)));

            id = IDManager.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex,count);
            assertTrue(IDManager.isTemporary(id));
            assertTrue(IDManager.VertexIDType.NormalVertex.is(id));

            id = IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserEdgeLabel,count);
            assertTrue(IDManager.isTemporary(id));
            assertTrue(IDManager.VertexIDType.UserEdgeLabel.is(id));

            id = IDManager.getTemporaryRelationID(count);
            assertTrue(IDManager.isTemporary(id));

            id = IDManager.getTemporaryVertexID(IDManager.VertexIDType.InvisibleVertex,count);
            assertTrue(IDManager.isTemporary(id));
            assertTrue(IDManager.VertexIDType.Invisible.is(id));

        }
    }

    @Test
    public void edgeTypeIDTest() {
        int partitionBits = 16;
        IDManager eid = new IDManager(partitionBits);
        int trails = 1000000;
        assertEquals(eid.getPartitionBound(), (1L << partitionBits));

        Serializer serializer = new StandardSerializer();
        for (int t = 0; t < trails; t++) {
            long count = RandomGenerator.randomLong(1, IDManager.getSchemaCountBound());
            long id;
            IDHandler.DirectionID dirID;
            RelationCategory type;
            if (Math.random() < 0.5) {
                id = IDManager.getSchemaId(IDManager.VertexIDType.UserEdgeLabel,count);
                assertTrue(eid.isEdgeLabelId(id));
                assertFalse(IDManager.isSystemRelationTypeId(id));
                type = RelationCategory.EDGE;
                if (Math.random() < 0.5)
                    dirID = IDHandler.DirectionID.EDGE_IN_DIR;
                else
                    dirID = IDHandler.DirectionID.EDGE_OUT_DIR;
            } else {
                type = RelationCategory.PROPERTY;
                id = IDManager.getSchemaId(IDManager.VertexIDType.UserPropertyKey, count);
                assertTrue(eid.isPropertyKeyId(id));
                assertFalse(IDManager.isSystemRelationTypeId(id));
                dirID = IDHandler.DirectionID.PROPERTY_DIR;
            }
            assertTrue(eid.isRelationTypeId(id));

            StaticBuffer b = IDHandler.getRelationType(id, dirID, false);
//            System.out.println(dirID);
//            System.out.println(getBinary(id));
//            System.out.println(getBuffer(b.asReadBuffer()));
            ReadBuffer rb = b.asReadBuffer();
            IDHandler.RelationTypeParse parse = IDHandler.readRelationType(rb);
            assertEquals(id,parse.typeId);
            assertEquals(dirID, parse.dirID);
            assertFalse(rb.hasRemaining());

            //Inline edge type
            WriteBuffer wb = new WriteByteBuffer(9);
            IDHandler.writeInlineRelationType(wb, id);
            long newId = IDHandler.readInlineRelationType(wb.getStaticBuffer().asReadBuffer());
            assertEquals(id,newId);

            //Compare to Kryo
            DataOutput out = serializer.getDataOutput(10);
            IDHandler.writeRelationType(out, id, dirID, false);
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
            IDHandler.writeInlineRelationType(out, t.longId());
        }
        for (long i=1;i<=numTries;i++) {
            IDHandler.writeInlineRelationType(out, IDManager.getSchemaId(IDManager.VertexIDType.UserEdgeLabel, i * 1000));
        }

        ReadBuffer in = out.getStaticBuffer().asReadBuffer();
        for (SystemRelationType t : SYSTEM_TYPES) {
            assertEquals(t, SystemTypeManager.getSystemType(IDHandler.readInlineRelationType(in)));
        }
        for (long i=1;i<=numTries;i++) {
            assertEquals(i * 1000, IDManager.stripEntireRelationTypePadding(IDHandler.readInlineRelationType(in)));
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
            testEdgeTypeWriting(t.longId());
        }
        for (int i=0;i<1000;i++) {
            IDManager.VertexIDType type = random.nextDouble()<0.5? IDManager.VertexIDType.UserPropertyKey: IDManager.VertexIDType.UserEdgeLabel;
            testEdgeTypeWriting(IDManager.getSchemaId(type,random.nextInt(1000000000)));
        }
    }

    public void testEdgeTypeWriting(long edgeTypeId) {
        IDHandler.DirectionID[] dir = IDManager.VertexIDType.EdgeLabel.is(edgeTypeId)?
                    new IDHandler.DirectionID[]{IDHandler.DirectionID.EDGE_IN_DIR, IDHandler.DirectionID.EDGE_OUT_DIR}:
                    new IDHandler.DirectionID[]{IDHandler.DirectionID.PROPERTY_DIR};
        boolean invisible = IDManager.isSystemRelationTypeId(edgeTypeId);
        for (IDHandler.DirectionID d : dir) {
            StaticBuffer b = IDHandler.getRelationType(edgeTypeId, d, invisible);
            IDHandler.RelationTypeParse parse = IDHandler.readRelationType(b.asReadBuffer());
            assertEquals(d,parse.dirID);
            assertEquals(edgeTypeId,parse.typeId);
        }
    }

    @Test
    public void testUserVertexBitWidth() {
        for (IDManager.VertexIDType type : IDManager.VertexIDType.values()) {
            assert !IDManager.VertexIDType.UserVertex.is(type.suffix()) || !type.isProper()
                    || type.offset() == IDManager.USERVERTEX_PADDING_BITWIDTH;
            assertTrue(type.offset()<=IDManager.MAX_PADDING_BITWIDTH);
        }
    }

    @Test
    public void partitionIDRangeTest() {

        List<PartitionIDRange> result = PartitionIDRange.getIDRanges(16, ImmutableList.of(getKeyRange(120<<16, 6, 140<<16, 8)));
        assertEquals(1, result.size());
        PartitionIDRange r = result.get(0);
        assertEquals(121,r.getLowerID());
        assertEquals(140,r.getUpperID());
        assertEquals(1<<16,r.getIdUpperBound());

        result = PartitionIDRange.getIDRanges(16, ImmutableList.of(getKeyRange(120<<16, 0, 140<<16, 0)));
        assertEquals(1, result.size());
        r = result.get(0);
        assertEquals(120,r.getLowerID());
        assertEquals(140,r.getUpperID());

        result = PartitionIDRange.getIDRanges(8, ImmutableList.of(getKeyRange(250<<24, 0, 0, 0)));
        assertEquals(1, result.size());
        r = result.get(0);
        assertEquals(250,r.getLowerID());
        assertEquals(0,r.getUpperID());

        for (int i=0;i<255;i=i+5) {
            result = PartitionIDRange.getIDRanges(8, ImmutableList.of(getKeyRange(i<<24, 0, i<<24, 0)));
            assertEquals(1, result.size());
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
        final StringBuilder result = new StringBuilder();
        while (r.hasRemaining()) {
            result.append(getBinary(VariableLong.unsignedByte(r.getByte()), 8)).append(" ");
        }
        return result.toString();
    }

    public String getBinary(long id) {
        return getBinary(id,64);
    }

    public String getBinary(long id, int normalizedLength) {
        final StringBuilder s = new StringBuilder(Long.toBinaryString(id));
        while (s.length() < normalizedLength) {
            s.insert(0, "0");
        }
        return s.toString();
    }

}
