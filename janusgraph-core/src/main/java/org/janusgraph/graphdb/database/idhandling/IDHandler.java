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

package org.janusgraph.graphdb.database.idhandling;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.RelationCategory;

import static org.janusgraph.graphdb.idmanagement.IDManager.VertexIDType.SystemEdgeLabel;
import static org.janusgraph.graphdb.idmanagement.IDManager.VertexIDType.SystemPropertyKey;
import static org.janusgraph.graphdb.idmanagement.IDManager.VertexIDType.UserEdgeLabel;
import static org.janusgraph.graphdb.idmanagement.IDManager.VertexIDType.UserPropertyKey;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDHandler {

    public static final StaticBuffer MIN_KEY = BufferUtil.getLongBuffer(0);
    public static final StaticBuffer MAX_KEY = BufferUtil.getLongBuffer(-1);

    public enum DirectionID {

        PROPERTY_DIR(0),  //00b
        EDGE_OUT_DIR(2),  //10b
        EDGE_IN_DIR(3);   //11b

        private final int id;

        DirectionID(int id) {
            this.id = id;
        }

        private int getRelationType() {
            return id >>> 1;
        }

        private int getDirectionInt() {
            return id & 1;
        }

        public RelationCategory getRelationCategory() {
            switch(this) {
                case PROPERTY_DIR:
                    return RelationCategory.PROPERTY;
                case EDGE_IN_DIR:
                case EDGE_OUT_DIR:
                    return RelationCategory.EDGE;
                default: throw new AssertionError();
            }
        }

        public Direction getDirection() {
            switch(this) {
                case PROPERTY_DIR:
                case EDGE_OUT_DIR:
                    return Direction.OUT;
                case EDGE_IN_DIR:
                    return Direction.IN;
                default: throw new AssertionError();
            }
        }

        private int getPrefix(boolean invisible, boolean systemType) {
            assert !systemType || invisible; // systemType implies invisible
            return ((systemType?0:invisible?2:1)<<1) + getRelationType();
        }

        private static DirectionID getDirectionID(int relationType, int direction) {
            assert relationType >= 0 && relationType <= 1 && direction >= 0 && direction <= 1;
            return forId((relationType << 1) + direction);
        }

        private static DirectionID forId(int id) {
            switch(id) {
                case 0: return PROPERTY_DIR;
                case 2: return EDGE_OUT_DIR;
                case 3: return EDGE_IN_DIR;
                default: throw new AssertionError("Invalid id: " + id);
            }
        }
    }


    private static final int PREFIX_BIT_LEN = 3;

    public static int relationTypeLength(long relationTypeId) {
        assert relationTypeId > 0 && (relationTypeId << 1) > 0;  //Check positive and no-overflow
        return VariableLong.positiveWithPrefixLength(IDManager.stripEntireRelationTypePadding(relationTypeId) << 1, PREFIX_BIT_LEN);
    }

    /**
     * The edge type is written as follows: [ Invisible &amp; System (2 bit) | Relation-Type-ID (1 bit) | Relation-Type-Count (variable) | Direction-ID (1 bit)]
     * Would only need 1 bit to store relation-type-id, but using two so we can upper bound.
     *
     *
     * @param out
     * @param relationTypeId
     * @param dirID
     */
    public static void writeRelationType(WriteBuffer out, long relationTypeId, DirectionID dirID, boolean invisible) {
        assert relationTypeId > 0 && (relationTypeId << 1) > 0; //Check positive and no-overflow

        long strippedId = (IDManager.stripEntireRelationTypePadding(relationTypeId) << 1) + dirID.getDirectionInt();
        VariableLong.writePositiveWithPrefix(out, strippedId, dirID.getPrefix(invisible, IDManager.isSystemRelationTypeId(relationTypeId)), PREFIX_BIT_LEN);
    }

    public static StaticBuffer getRelationType(long relationTypeId, DirectionID dirID, boolean invisible) {
        WriteBuffer b = new WriteByteBuffer(relationTypeLength(relationTypeId));
        IDHandler.writeRelationType(b, relationTypeId, dirID, invisible);
        return b.getStaticBuffer();
    }

    public static RelationTypeParse readRelationType(ReadBuffer in) {
        long[] countPrefix = VariableLong.readPositiveWithPrefix(in, PREFIX_BIT_LEN);
        DirectionID dirID = DirectionID.getDirectionID((int) countPrefix[1] & 1, (int) (countPrefix[0] & 1));
        long typeId = countPrefix[0] >>> 1;
        boolean isSystemType = (countPrefix[1]>>1)==0;

        if (dirID == DirectionID.PROPERTY_DIR)
            typeId = IDManager.getSchemaId(isSystemType?SystemPropertyKey:UserPropertyKey, typeId);
        else
            typeId = IDManager.getSchemaId(isSystemType?SystemEdgeLabel:UserEdgeLabel, typeId);
        return new RelationTypeParse(typeId,dirID);
    }

    public static class RelationTypeParse {

        public final long typeId;
        public final DirectionID dirID;

        public RelationTypeParse(long typeId, DirectionID dirID) {
            this.typeId = typeId;
            this.dirID = dirID;
        }
    }


    public static void writeInlineRelationType(WriteBuffer out, long relationTypeId) {
        long compressId = IDManager.stripRelationTypePadding(relationTypeId);
        VariableLong.writePositive(out, compressId);
    }

    public static long readInlineRelationType(ReadBuffer in) {
        long compressId = VariableLong.readPositive(in);
        return IDManager.addRelationTypePadding(compressId);
    }

    private static StaticBuffer getPrefixed(int prefix) {
        assert prefix < (1 << PREFIX_BIT_LEN) && prefix >= 0;
        byte[] arr = new byte[1];
        arr[0] = (byte) (prefix << (Byte.SIZE - PREFIX_BIT_LEN));
        return new StaticArrayBuffer(arr);
    }

    public static StaticBuffer[] getBounds(RelationCategory type, boolean systemTypes) {
        int start, end;
        switch (type) {
            case PROPERTY:
                start = DirectionID.PROPERTY_DIR.getPrefix(systemTypes,systemTypes);
                end = start;
                break;
            case EDGE:
                start = DirectionID.EDGE_OUT_DIR.getPrefix(systemTypes,systemTypes);
                end = start;
                break;
            case RELATION:
                start = DirectionID.PROPERTY_DIR.getPrefix(systemTypes,systemTypes);
                end = DirectionID.EDGE_OUT_DIR.getPrefix(systemTypes,systemTypes);
                break;
            default:
                throw new AssertionError("Unrecognized type:" + type);
        }
        end++;
        assert end > start;
        return new StaticBuffer[]{getPrefixed(start), getPrefixed(end)};
    }

}
