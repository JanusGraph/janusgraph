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

package org.janusgraph.graphdb.database;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongSet;
import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.InternalAttributeUtil;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.util.datastructures.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

import static org.janusgraph.graphdb.database.idhandling.IDHandler.DirectionID;
import static org.janusgraph.graphdb.database.idhandling.IDHandler.RelationTypeParse;
import static org.janusgraph.graphdb.database.idhandling.IDHandler.getBounds;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class EdgeSerializer implements RelationReader {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(EdgeSerializer.class);


    private static final int DEFAULT_COLUMN_CAPACITY = 60;
    private static final int DEFAULT_CAPACITY = 128;

    private final Serializer serializer;

    public EdgeSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public RelationCache readRelation(Entry data, boolean parseHeaderOnly, TypeInspector tx) {
        RelationCache map = data.getCache();
        if (map == null || !(parseHeaderOnly || map.hasProperties())) {
            map = parseRelation(data, parseHeaderOnly, tx);
            data.setCache(map);
        }
        return map;
    }

    public Direction parseDirection(Entry data) {
        RelationCache map = data.getCache();
        if (map != null) return map.direction;
        return IDHandler.readRelationType(data.asReadBuffer()).dirID.getDirection();
    }

    @Override
    public RelationCache parseRelation(Entry data, boolean excludeProperties, TypeInspector tx) {
        ReadBuffer in = data.asReadBuffer();

        RelationTypeParse typeAndDir = IDHandler.readRelationType(in);

        long typeId = typeAndDir.typeId;
        Direction dir = typeAndDir.dirID.getDirection();

        RelationType relationType = tx.getExistingRelationType(typeId);
        InternalRelationType def = (InternalRelationType) relationType;
        Multiplicity multiplicity = def.multiplicity();
        long[] keySignature = def.getSortKey();

        long relationId;
        Object other;
        int startKeyPos = in.getPosition();
        int endKeyPos = 0;
        if (relationType.isEdgeLabel()) {
            long otherVertexId;
            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) {
                    otherVertexId = VariableLong.readPositive(in);
                } else {
                    in.movePositionTo(data.getValuePosition());
                    otherVertexId = VariableLong.readPositiveBackward(in);
                    in.movePositionTo(data.getValuePosition());
                }
                relationId = VariableLong.readPositive(in);
            } else {
                in.movePositionTo(data.getValuePosition());

                relationId = VariableLong.readPositiveBackward(in);
                otherVertexId = VariableLong.readPositiveBackward(in);
                endKeyPos = in.getPosition();
                in.movePositionTo(data.getValuePosition());
            }
            other = otherVertexId;
        } else {
            assert relationType.isPropertyKey();
            PropertyKey key = (PropertyKey) relationType;

            if (multiplicity.isConstrained()) {
                other = readPropertyValue(in,key);
                relationId = VariableLong.readPositive(in);
            } else {
                in.movePositionTo(data.getValuePosition());
                relationId = VariableLong.readPositiveBackward(in);
                endKeyPos = in.getPosition();
                in.movePositionTo(data.getValuePosition());
                other = readPropertyValue(in,key);
            }
            Preconditions.checkNotNull(other,
                "Encountered error in deserializer [null value returned]. Check serializer compatibility.");
        }

        if (!excludeProperties) {

            LongObjectHashMap<Object> properties = new LongObjectHashMap<>(4);

            if (!multiplicity.isConstrained() && keySignature.length > 0) {
                int currentPos = in.getPosition();
                //Read sort key which only exists if type is not unique in this direction
                assert endKeyPos > startKeyPos;
                int keyLength = endKeyPos - startKeyPos; //after reading the ids, we are on the last byte of the key
                in.movePositionTo(startKeyPos);
                ReadBuffer inKey = in;
                if (def.getSortOrder() == Order.DESC) inKey = in.subrange(keyLength, true);
                readInlineTypes(keySignature, properties, inKey, tx, InlineType.KEY);
                in.movePositionTo(currentPos);
            }

            //read value signature
            readInlineTypes(def.getSignature(), properties, in, tx, InlineType.SIGNATURE);

            //Third: read rest
            while (in.hasRemaining()) {
                PropertyKey type = tx.getExistingPropertyKey(IDHandler.readInlineRelationType(in));
                Object propertyValue = readInline(in, type, InlineType.NORMAL);
                assert propertyValue != null;
                properties.put(type.longId(), propertyValue);
            }

            if (data.hasMetaData()) {
                for (Map.Entry<EntryMetaData,Object> metas : data.getMetaData().entrySet()) {
                    ImplicitKey key = ImplicitKey.MetaData2ImplicitKey.get(metas.getKey());
                    if (key != null) {
                        assert metas.getValue() != null;
                        properties.put(key.longId(),metas.getValue());
                    }
                }
            }

            return new RelationCache(dir, typeId, relationId, other, properties);
        } else {
            return new RelationCache(dir, typeId, relationId, other);
        }
    }

    private void readInlineTypes(long[] keyIds, LongObjectHashMap<Object> properties, ReadBuffer in, TypeInspector tx,
                                 InlineType inlineType) {
        for (long keyId : keyIds) {
            PropertyKey keyType = tx.getExistingPropertyKey(keyId);
            Object value = readInline(in, keyType, inlineType);
            if (value != null) properties.put(keyId, value);
        }
    }

    private Object readInline(ReadBuffer read, PropertyKey key, InlineType inlineType) {
        return readPropertyValue(read, key, inlineType);
    }

    private Object readPropertyValue(ReadBuffer read, PropertyKey key) {
        return readPropertyValue(read,key,InlineType.NORMAL);
    }

    private Object readPropertyValue(ReadBuffer read, PropertyKey key, InlineType inlineType) {
        if (InternalAttributeUtil.hasGenericDataType(key)) {
            return serializer.readClassAndObject(read);
        } else {
            if (inlineType.writeByteOrdered())
                return serializer.readObjectByteOrder(read, key.dataType());
            else
                return serializer.readObject(read, key.dataType());
        }
    }

    private static DirectionID getDirID(Direction dir, RelationCategory rt) {
        switch (rt) {
            case PROPERTY:
                assert dir == Direction.OUT;
                return DirectionID.PROPERTY_DIR;

            case EDGE:
                switch (dir) {
                    case OUT:
                        return DirectionID.EDGE_OUT_DIR;

                    case IN:
                        return DirectionID.EDGE_IN_DIR;

                    default:
                        throw new IllegalArgumentException("Invalid direction: " + dir);
                }

            default:
                throw new IllegalArgumentException("Invalid relation type: " + rt);
        }
    }

    public Entry writeRelation(InternalRelation relation, int position, TypeInspector tx) {
        return writeRelation(relation, (InternalRelationType) relation.getType(), position, tx);
    }

    public StaticArrayEntry writeRelation(InternalRelation relation, InternalRelationType type, int position,
                                          TypeInspector tx) {
        assert type==relation.getType() || (type.getBaseType() != null
                && type.getBaseType().equals(relation.getType()));
        Direction dir = EdgeDirection.fromPosition(position);
        Preconditions.checkArgument(type.isUnidirected(Direction.BOTH) || type.isUnidirected(dir));
        long typeId = type.longId();
        DirectionID dirID = getDirID(dir, relation.isProperty() ? RelationCategory.PROPERTY : RelationCategory.EDGE);

        DataOutput out = serializer.getDataOutput(DEFAULT_CAPACITY);
        int valuePosition;
        IDHandler.writeRelationType(out, typeId, dirID, type.isInvisibleType());
        Multiplicity multiplicity = type.multiplicity();

        long[] sortKey = type.getSortKey();
        assert !multiplicity.isConstrained() || sortKey.length==0: type.name();
        int keyStartPos = out.getPosition();
        if (!multiplicity.isConstrained()) {
            writeInlineTypes(sortKey, relation, out, tx, InlineType.KEY);
        }
        int keyEndPos = out.getPosition();

        long relationId = relation.longId();

        //How multiplicity is handled for edges and properties is slightly different
        if (relation.isEdge()) {
            long otherVertexId = relation.getVertex((position + 1) % 2).longId();
            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) {
                    valuePosition = out.getPosition();
                    VariableLong.writePositive(out, otherVertexId);
                } else {
                    VariableLong.writePositiveBackward(out, otherVertexId);
                    valuePosition = out.getPosition();
                }
                VariableLong.writePositive(out, relationId);
            } else {
                VariableLong.writePositiveBackward(out, otherVertexId);
                VariableLong.writePositiveBackward(out, relationId);
                valuePosition = out.getPosition();
            }
        } else {
            Preconditions.checkArgument(relation.isProperty(), "Given relation is not property");
            Object value = ((JanusGraphVertexProperty) relation).value();
            Preconditions.checkNotNull(value);
            PropertyKey key = (PropertyKey) type;
            assert key.dataType().isInstance(value);

            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) { //Cardinality=SINGLE
                    valuePosition = out.getPosition();
                    writePropertyValue(out,key,value);
                } else { //Cardinality=SET
                    writePropertyValue(out,key,value);
                    valuePosition = out.getPosition();
                }
                VariableLong.writePositive(out, relationId);
            } else {
                assert multiplicity.getCardinality()== Cardinality.LIST;
                VariableLong.writePositiveBackward(out, relationId);
                valuePosition = out.getPosition();
                writePropertyValue(out,key,value);
            }
        }

        //Write signature
        long[] signature = type.getSignature();
        writeInlineTypes(signature, relation, out, tx, InlineType.SIGNATURE);

        //Write remaining properties
        LongSet writtenTypes = new LongHashSet(sortKey.length + signature.length);
        if (sortKey.length > 0 || signature.length > 0) {
            for (long id : sortKey) writtenTypes.add(id);
            for (long id : signature) writtenTypes.add(id);
        }
        LongArrayList remainingTypes = new LongArrayList(8);
        for (PropertyKey t : relation.getPropertyKeysDirect()) {
            if (!(t instanceof ImplicitKey) && !writtenTypes.contains(t.longId())) {
                remainingTypes.add(t.longId());
            }
        }
        //Sort types before writing to ensure that value is always written the same way
        long[] remaining = remainingTypes.toArray();
        Arrays.sort(remaining);
        for (long tid : remaining) {
            PropertyKey t = tx.getExistingPropertyKey(tid);
            writeInline(out, t, relation.getValueDirect(t), InlineType.NORMAL);
        }
        assert valuePosition>0;

        return new StaticArrayEntry(type.getSortOrder() == Order.DESC ?
                                    out.getStaticBufferFlipBytes(keyStartPos, keyEndPos) :
                                    out.getStaticBuffer(), valuePosition);
    }

    private enum InlineType {

        KEY, SIGNATURE, NORMAL;

        public boolean writeInlineKey() {
            return this==NORMAL;
        }

        public boolean writeByteOrdered() {
            return this==KEY;
        }

    }

    private void writeInlineTypes(long[] keyIds, InternalRelation relation, DataOutput out, TypeInspector tx,
                                  InlineType inlineType) {
        for (long keyId : keyIds) {
            PropertyKey t = tx.getExistingPropertyKey(keyId);
            writeInline(out, t, relation.getValueDirect(t), inlineType);
        }
    }

    private void writeInline(DataOutput out, PropertyKey inlineKey, Object value, InlineType inlineType) {
        assert inlineType.writeInlineKey() || !InternalAttributeUtil.hasGenericDataType(inlineKey);

        if (inlineType.writeInlineKey()) {
            IDHandler.writeInlineRelationType(out, inlineKey.longId());
        }

        writePropertyValue(out,inlineKey,value, inlineType);
    }

    private void writePropertyValue(DataOutput out, PropertyKey key, Object value) {
        writePropertyValue(out,key,value,InlineType.NORMAL);
    }

    private void writePropertyValue(DataOutput out, PropertyKey key, Object value, InlineType inlineType) {
        if (InternalAttributeUtil.hasGenericDataType(key)) {
            assert !inlineType.writeByteOrdered();
            out.writeClassAndObject(value);
        } else {
            assert value==null || value.getClass().equals(key.dataType());
            if (inlineType.writeByteOrdered()) out.writeObjectByteOrder(value, key.dataType());
            else out.writeObject(value, key.dataType());
        }
    }

    public SliceQuery getQuery(RelationCategory resultType, boolean querySystemTypes) {
        Preconditions.checkNotNull(resultType);
        StaticBuffer[] bound = getBounds(resultType, querySystemTypes);
        return new SliceQuery(bound[0], bound[1]);
    }

    public SliceQuery getQuery(InternalRelationType type, Direction dir, TypedInterval[] sortKey) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(dir);
        Preconditions.checkArgument(type.isUnidirected(Direction.BOTH) || type.isUnidirected(dir));


        StaticBuffer sliceStart = null, sliceEnd = null;
        RelationCategory rt = type.isPropertyKey() ? RelationCategory.PROPERTY : RelationCategory.EDGE;
        if (dir == Direction.BOTH) {
            assert type.isEdgeLabel();
            sliceStart = IDHandler.getRelationType(type.longId(), getDirID(Direction.OUT, rt), type.isInvisibleType());
            sliceEnd = IDHandler.getRelationType(type.longId(), getDirID(Direction.IN, rt), type.isInvisibleType());
            assert sliceStart.compareTo(sliceEnd)<0;
            sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
        } else {
            DirectionID dirID = getDirID(dir, rt);

            DataOutput colStart = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY);
            DataOutput colEnd = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY);
            IDHandler.writeRelationType(colStart, type.longId(), dirID, type.isInvisibleType());
            IDHandler.writeRelationType(colEnd, type.longId(), dirID, type.isInvisibleType());

            long[] sortKeyIDs = type.getSortKey();
            Preconditions.checkArgument(sortKey.length >= sortKeyIDs.length);
            assert colStart.getPosition() == colEnd.getPosition();
            int keyStartPos = colStart.getPosition();
            int keyEndPos = -1;
            for (int i = 0; i < sortKey.length && sortKey[i] != null; i++) {
                PropertyKey propertyKey = sortKey[i].key;
                Interval interval = sortKey[i].interval;

                if (i>=sortKeyIDs.length) {
                    assert !type.multiplicity().isUnique(dir);
                    assert propertyKey==ImplicitKey.JANUSGRAPHID || propertyKey==ImplicitKey.ADJACENT_ID;
                    assert propertyKey!=ImplicitKey.ADJACENT_ID || (i==sortKeyIDs.length);
                    assert propertyKey!=ImplicitKey.JANUSGRAPHID || (!type.multiplicity().isConstrained() &&
                                                  (i==sortKeyIDs.length || i==sortKeyIDs.length+1));
                    assert colStart.getPosition()==colEnd.getPosition();
                    assert interval==null || interval.isPoints();
                    keyEndPos = colStart.getPosition();

                } else {
                    assert !type.multiplicity().isConstrained();
                    assert propertyKey.longId() == sortKeyIDs[i];
                }

                if (interval == null || interval.isEmpty()) {
                    break;
                }
                if (interval.isPoints()) {
                    if (propertyKey==ImplicitKey.JANUSGRAPHID || propertyKey==ImplicitKey.ADJACENT_ID) {
                        assert !type.multiplicity().isUnique(dir);
                        VariableLong.writePositiveBackward(colStart, (Long)interval.getStart());
                        VariableLong.writePositiveBackward(colEnd, (Long)interval.getEnd());
                    } else {
                        writeInline(colStart, propertyKey, interval.getStart(), InlineType.KEY);
                        writeInline(colEnd, propertyKey, interval.getEnd(), InlineType.KEY);
                    }
                } else {
                    if (interval.getStart() != null)
                        writeInline(colStart, propertyKey, interval.getStart(), InlineType.KEY);
                    if (interval.getEnd() != null)
                        writeInline(colEnd, propertyKey, interval.getEnd(), InlineType.KEY);

                    switch (type.getSortOrder()) {
                        case ASC:
                            sliceStart = colStart.getStaticBuffer();
                            sliceEnd = colEnd.getStaticBuffer();
                            if (!interval.startInclusive()) sliceStart = BufferUtil.nextBiggerBuffer(sliceStart);
                            if (interval.endInclusive()) sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
                            break;

                        case DESC:
                            sliceEnd = colStart.getStaticBufferFlipBytes(keyStartPos,colStart.getPosition());
                            sliceStart = colEnd.getStaticBufferFlipBytes(keyStartPos,colEnd.getPosition());
                            if (interval.startInclusive()) sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
                            if (!interval.endInclusive()) sliceStart = BufferUtil.nextBiggerBuffer(sliceStart);
                            break;

                        default:
                            throw new AssertionError(type.getSortOrder().toString());
                    }

                    assert sliceStart.compareTo(sliceEnd)<=0;
                    break;
                }
            }
            if (sliceStart==null) {
                assert sliceEnd==null && colStart.getPosition()==colEnd.getPosition();
                if (keyEndPos<0) keyEndPos=colStart.getPosition();
                switch (type.getSortOrder()) {
                    case ASC:
                        sliceStart = colStart.getStaticBuffer();
                        break;

                    case DESC:
                        sliceStart = colStart.getStaticBufferFlipBytes(keyStartPos,keyEndPos);
                        break;

                    default:
                        throw new AssertionError(type.getSortOrder().toString());
                }
                sliceEnd = BufferUtil.nextBiggerBuffer(sliceStart);
            }
        }
        return new SliceQuery(sliceStart, sliceEnd, type.name());
    }

    public static class TypedInterval {
        public final PropertyKey key;
        public final Interval interval;


        public TypedInterval(PropertyKey key, Interval interval) {
            this.key = key;
            this.interval = interval;
        }
    }

}
