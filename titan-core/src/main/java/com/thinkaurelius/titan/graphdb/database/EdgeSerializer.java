package com.thinkaurelius.titan.graphdb.database;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.LongSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.util.datastructures.Interval;
import com.tinkerpop.blueprints.Direction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

import static com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler.*;

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
        return IDHandler.readEdgeType(data.asReadBuffer()).dirID.getDirection();
    }

    @Override
    public RelationCache parseRelation(Entry data, boolean excludeProperties, TypeInspector tx) {
        ReadBuffer in = data.asReadBuffer();

        LongObjectOpenHashMap properties = excludeProperties ? null : new LongObjectOpenHashMap(4);
        EdgeTypeParse typeAndDir = IDHandler.readEdgeType(in);

        long typeId = typeAndDir.typeId;
        Direction dir = typeAndDir.dirID.getDirection();
        RelationCategory rtype = typeAndDir.dirID.getRelationCategory();

        RelationType relationType = tx.getExistingRelationType(typeId);
        InternalRelationType def = (InternalRelationType) relationType;
        Multiplicity multiplicity = def.getMultiplicity();
        long[] keysig = def.getSortKey();

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
        }
        assert other!=null;

        if (!excludeProperties && !multiplicity.isConstrained() && keysig.length>0) {
            int currentPos = in.getPosition();
            //Read sort key which only exists if type is not unique in this direction
            assert endKeyPos>startKeyPos;
            int keyLength = endKeyPos-startKeyPos; //after reading the ids, we are on the last byte of the key
            in.movePositionTo(startKeyPos);
            ReadBuffer inkey = in;
            if (def.getSortOrder()==Order.DESC) inkey = in.subrange(keyLength,true);
            readInlineTypes(keysig, properties, inkey, tx, InlineType.KEY);
            in.movePositionTo(currentPos);
        }

        if (!excludeProperties) {
            //read value signature
            readInlineTypes(def.getSignature(), properties, in, tx, InlineType.SIGNATURE);

            //Third: read rest
            while (in.hasRemaining()) {
                RelationType type = tx.getExistingRelationType(IDHandler.readInlineEdgeType(in));
                Object pvalue = readInline(in, type, InlineType.NORMAL);
                assert pvalue != null;
                properties.put(type.getID(), pvalue);
            }

            if (data.hasMetaData()) {
                for (Map.Entry<EntryMetaData,Object> metas : data.getMetaData().entrySet()) {
                    ImplicitKey key = ImplicitKey.MetaData2ImplicitKey.get(metas.getKey());
                    if (key!=null) {
                        assert metas.getValue()!=null;
                        properties.put(key.getID(),metas.getValue());
                    }
                }
            }
        }

        return new RelationCache(dir, typeId, relationId, other, properties);
    }

    private void readInlineTypes(long[] typeids, LongObjectOpenHashMap properties, ReadBuffer in, TypeInspector tx, InlineType inlineType) {
        for (long typeid : typeids) {
            RelationType keyType = tx.getExistingRelationType(typeid);
            Object value = readInline(in, keyType, inlineType);
            if (value != null) properties.put(typeid, value);
        }
    }

    private Object readInline(ReadBuffer read, RelationType type, InlineType inlineType) {
        if (type.isPropertyKey()) {
            PropertyKey key = ((PropertyKey) type);
            return readPropertyValue(read,key, inlineType);
        } else {
            assert type.isEdgeLabel();
            long id;
            if (inlineType.writeByteOrdered())
                id = LongSerializer.INSTANCE.readByteOrder(read);
            else
                id = VariableLong.readPositive(read);
            return id == 0 ? null : id;
        }
    }

    private Object readPropertyValue(ReadBuffer read, PropertyKey key) {
        return readPropertyValue(read,key,InlineType.NORMAL);
    }

    private Object readPropertyValue(ReadBuffer read, PropertyKey key, InlineType inlineType) {
        if (AttributeUtil.hasGenericDataType(key)) {
            return serializer.readClassAndObject(read);
        } else {
            if (inlineType.writeByteOrdered())
                return serializer.readObjectByteOrder(read, key.getDataType());
            else
                return serializer.readObject(read, key.getDataType());
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

    public Entry writeRelation(InternalRelation relation, InternalRelationType type, int position, TypeInspector tx) {
        assert type==relation.getType() || type.getBaseType().equals(relation.getType());
        Direction dir = EdgeDirection.fromPosition(position);
        Preconditions.checkArgument(type.isUnidirected(Direction.BOTH) || type.isUnidirected(dir));
        long typeid = type.getID();
        DirectionID dirID = getDirID(dir, relation.isProperty() ? RelationCategory.PROPERTY : RelationCategory.EDGE);

        DataOutput out = serializer.getDataOutput(DEFAULT_CAPACITY);
        int valuePosition;
        IDHandler.writeEdgeType(out, typeid, dirID, type.isHiddenType());
        Multiplicity multiplicity = type.getMultiplicity();

        long[] sortKey = type.getSortKey();
        assert !multiplicity.isConstrained() || sortKey.length==0: type.getName();
        int keyStartPos = out.getPosition();
        if (!multiplicity.isConstrained()) {
            writeInlineTypes(sortKey, relation, out, tx, InlineType.KEY);
        }
        int keyEndPos = out.getPosition();


        long relationId = relation.getID();
        //How multiplicity is handled for edges and properties is slightly different
        if (relation.isEdge()) {
            long otherVertexId = relation.getVertex((position + 1) % 2).getID();
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
            assert relation.isProperty();
            Preconditions.checkArgument(relation.isProperty());
            Object value = ((TitanProperty) relation).getValue();
            Preconditions.checkNotNull(value);
            PropertyKey key = (PropertyKey) type;
            assert key.getDataType().isInstance(value);

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
        LongSet writtenTypes = new LongOpenHashSet(sortKey.length + signature.length);
        boolean hasImplicitKeys = false;
        if (sortKey.length > 0 || signature.length > 0) {
            for (long id : sortKey) writtenTypes.add(id);
            for (long id : signature) writtenTypes.add(id);
        }
        LongArrayList remainingTypes = new LongArrayList(8);
        for (RelationType t : relation.getPropertyKeysDirect()) {
            if (t instanceof ImplicitKey) {
                hasImplicitKeys=true;
            } else if (!writtenTypes.contains(t.getID())) {
                remainingTypes.add(t.getID());
            }
        }
        //Sort types before writing to ensure that value is always written the same way
        long[] remaining = remainingTypes.toArray();
        Arrays.sort(remaining);
        for (long tid : remaining) {
            RelationType t = tx.getExistingRelationType(tid);
            writeInline(out, t, relation.getProperty(t), InlineType.NORMAL);
        }
        assert valuePosition>0;

        StaticArrayEntry entry = new StaticArrayEntry(type.getSortOrder()==Order.DESC?
                                    out.getStaticBufferFlipBytes(keyStartPos,keyEndPos):
                                    out.getStaticBuffer(),valuePosition);
        if (hasImplicitKeys) {
            for (EntryMetaData meta : EntryMetaData.IDENTIFYING_METADATA) {
                Object value = relation.getPropertyDirect(ImplicitKey.MetaData2ImplicitKey.get(meta));
                if (value!=null) entry.setMetaData(meta,value);
            }
        }
        return entry;
    }

    private enum InlineType {

        KEY, SIGNATURE, NORMAL;

        public boolean writeEdgeType() {
            return this==NORMAL;
        }

        public boolean writeByteOrdered() {
            return this==KEY;
        }

    }

    private void writeInlineTypes(long[] typeids, InternalRelation relation, DataOutput out, TypeInspector tx, InlineType inlineType) {
        for (long typeid : typeids) {
            RelationType t = tx.getExistingRelationType(typeid);
            writeInline(out, t, relation.getProperty(t), inlineType);
        }
    }

    private void writeInline(DataOutput out, RelationType type, Object value, InlineType inlineType) {
        assert !(type.isPropertyKey() && !inlineType.writeEdgeType()) || !AttributeUtil.hasGenericDataType((PropertyKey) type);

        if (inlineType.writeEdgeType()) {
            IDHandler.writeInlineEdgeType(out, type.getID());
        }

        if (type.isPropertyKey()) {
            writePropertyValue(out,(PropertyKey)type,value, inlineType);
        } else {
            assert type.isEdgeLabel() && ((EdgeLabel) type).isUnidirected();
            long id = (value==null?0:((InternalVertex) value).getID());
            if (inlineType.writeByteOrdered()) LongSerializer.INSTANCE.writeByteOrder(out,id);
            else VariableLong.writePositive(out,id);
        }
    }

    private void writePropertyValue(DataOutput out, PropertyKey key, Object value) {
        writePropertyValue(out,key,value,InlineType.NORMAL);
    }

    private void writePropertyValue(DataOutput out, PropertyKey key, Object value, InlineType inlineType) {
        if (AttributeUtil.hasGenericDataType(key)) {
            assert !inlineType.writeByteOrdered();
            out.writeClassAndObject(value);
        } else {
            assert value==null || value.getClass().equals(key.getDataType());
            if (inlineType.writeByteOrdered()) out.writeObjectByteOrder(value, key.getDataType());
            else out.writeObject(value, key.getDataType());
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
            sliceStart = IDHandler.getEdgeType(type.getID(), getDirID(Direction.OUT, rt),type.isHiddenType());
            sliceEnd = IDHandler.getEdgeType(type.getID(), getDirID(Direction.IN, rt),type.isHiddenType());
            assert sliceStart.compareTo(sliceEnd)<0;
            sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
        } else {
            DirectionID dirID = getDirID(dir, rt);

            DataOutput colStart = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY);
            DataOutput colEnd = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY);
            IDHandler.writeEdgeType(colStart, type.getID(), dirID, type.isHiddenType());
            IDHandler.writeEdgeType(colEnd, type.getID(), dirID, type.isHiddenType());

            long[] sortKeyIDs = type.getSortKey();
            Preconditions.checkArgument(sortKey.length >= sortKeyIDs.length);
            assert colStart.getPosition() == colEnd.getPosition();
            int keyStartPos = colStart.getPosition();
            int keyEndPos = -1;
            for (int i = 0; i < sortKey.length && sortKey[i] != null; i++) {
                RelationType t = sortKey[i].type;
                Interval interval = sortKey[i].interval;

                if (i>=sortKeyIDs.length) {
                    assert !type.getMultiplicity().isUnique(dir);
                    assert (t instanceof ImplicitKey) && (t==ImplicitKey.TITANID || t==ImplicitKey.ADJACENT_ID);
                    assert t!=ImplicitKey.ADJACENT_ID || (i==sortKeyIDs.length);
                    assert t!=ImplicitKey.TITANID || (!type.getMultiplicity().isConstrained() &&
                                                  (i==sortKeyIDs.length && t.isPropertyKey() || i==sortKeyIDs.length+1 && t.isEdgeLabel() ));
                    assert colStart.getPosition()==colEnd.getPosition();
                    assert interval==null || interval.isPoint();
                    keyEndPos = colStart.getPosition();

                } else {
                    assert !type.getMultiplicity().isConstrained();
                    assert t.getID() == sortKeyIDs[i];
                }

                if (interval == null || interval.isEmpty()) {
                    break;
                }
                if (interval.isPoint()) {
                    if (t==ImplicitKey.TITANID || t==ImplicitKey.ADJACENT_ID) {
                        assert !type.getMultiplicity().isUnique(dir);
                        VariableLong.writePositiveBackward(colStart, (Long)interval.getStart());
                        VariableLong.writePositiveBackward(colEnd, (Long)interval.getEnd());
                    } else {
                        writeInline(colStart, t, interval.getStart(), InlineType.KEY);
                        writeInline(colEnd, t, interval.getEnd(), InlineType.KEY);
                    }
                } else {
                    if (interval.getStart() != null)
                        writeInline(colStart, t, interval.getStart(), InlineType.KEY);
                    if (interval.getEnd() != null)
                        writeInline(colEnd, t, interval.getEnd(), InlineType.KEY);

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

                        default: throw new AssertionError(type.getSortOrder().toString());
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

                    default: throw new AssertionError(type.getSortOrder().toString());
                }
                sliceEnd = BufferUtil.nextBiggerBuffer(sliceStart);
            }
        }
        return new SliceQuery(sliceStart, sliceEnd);
    }

    public static class TypedInterval {
        public final InternalRelationType type;
        public final Interval interval;


        public TypedInterval(InternalRelationType type, Interval interval) {
            this.type = type;
            this.interval = interval;
        }
    }

}
