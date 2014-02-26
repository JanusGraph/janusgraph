package com.thinkaurelius.titan.graphdb.database;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
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
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.util.datastructures.Interval;
import com.tinkerpop.blueprints.Direction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

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

    public RelationCache readRelation(long vertexid, Entry data, boolean parseHeaderOnly, TypeInspector tx) {
        RelationCache map = data.getCache();
        if (map == null || !(parseHeaderOnly || map.hasProperties())) {
            map = parseRelation(vertexid, data, parseHeaderOnly, tx);
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
    public RelationCache parseRelation(long vertexid, Entry data, boolean excludeProperties, TypeInspector tx) {
        assert vertexid > 0;

        ReadBuffer in = data.asReadBuffer();

        LongObjectOpenHashMap properties = excludeProperties ? null : new LongObjectOpenHashMap(4);
        EdgeTypeParse typeAndDir = IDHandler.readEdgeType(in);

        long typeId = typeAndDir.typeId;
        Direction dir = typeAndDir.dirID.getDirection();
        RelationCategory rtype = typeAndDir.dirID.getRelationCategory();

        TitanType titanType = tx.getExistingType(typeId);
        InternalRelationType def = (InternalRelationType) titanType;
        Multiplicity multiplicity = def.getMultiplicity();
        long[] keysig = def.getSortKey();

        long relationIdDiff;
        Object other;
        int startKeyPos = in.getPosition();
        int endKeyPos = 0;
        if (titanType.isEdgeLabel()) {
            long vertexIdDiff;
            if (multiplicity.isConstrained()) {
                vertexIdDiff = VariableLong.read(in);
                relationIdDiff = VariableLong.read(in);
            } else {
                in.movePositionTo(data.getValuePosition());
                relationIdDiff = VariableLong.readBackward(in);
                vertexIdDiff = VariableLong.readBackward(in);
                endKeyPos = in.getPosition();
                in.movePositionTo(data.getValuePosition());
            }
            other = vertexid + vertexIdDiff;
        } else {
            assert titanType.isPropertyKey();
            TitanKey key = (TitanKey) titanType;

            if (multiplicity.isConstrained()) {
                other = readPropertyValue(in,key);
                relationIdDiff = VariableLong.read(in);
            } else {
                in.movePositionTo(data.getValuePosition());
                relationIdDiff = VariableLong.readBackward(in);
                endKeyPos = in.getPosition();
                in.movePositionTo(data.getValuePosition());
                other = readPropertyValue(in,key);
            }
        }
        assert other!=null;
        assert relationIdDiff + vertexid > 0;
        long relationId = relationIdDiff + vertexid;

        if (!excludeProperties && !multiplicity.isConstrained() && keysig.length>0) {
            int currentPos = in.getPosition();
            //Read sort key which only exists if type is not unique in this direction
            assert endKeyPos>startKeyPos;
            int keyLength = endKeyPos-startKeyPos; //after reading the ids, we are on the last byte of the key
            in.movePositionTo(startKeyPos);
            ReadBuffer inkey = in;
            if (def.getSortOrder()==Order.DESC) inkey = in.subrange(keyLength,true);
            readInlineTypes(keysig, properties, inkey, tx);
            in.movePositionTo(currentPos);
        }

        if (!excludeProperties) {
            //read value signature
            readInlineTypes(def.getSignature(), properties, in, tx);

            //Third: read rest
            while (in.hasRemaining()) {
                TitanType type = tx.getExistingType(IDHandler.readInlineEdgeType(in));
                Object pvalue = readInline(in, type);
                assert pvalue != null;
                properties.put(type.getID(), pvalue);
            }
        }
        return new RelationCache(dir, typeId, relationId, other, properties);
    }

    private void readInlineTypes(long[] typeids, LongObjectOpenHashMap properties, ReadBuffer in, TypeInspector tx) {
        for (long typeid : typeids) {
            TitanType keyType = tx.getExistingType(typeid);
            Object value = readInline(in, keyType);
            if (value != null) properties.put(typeid, value);
        }
    }

    private Object readInline(ReadBuffer read, TitanType type) {
        if (type.isPropertyKey()) {
            TitanKey key = ((TitanKey) type);
            return readPropertyValue(read,key);
        } else {
            assert type.isEdgeLabel();
            long id = VariableLong.readPositive(read);
            return id == 0 ? null : id;
        }
    }

    private Object readPropertyValue(ReadBuffer read, TitanKey key) {
        return AttributeUtil.hasGenericDataType(key)
                ? serializer.readClassAndObject(read)
                : serializer.readObject(read, key.getDataType());
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

    private void writeInlineTypes(long[] typeids, InternalRelation relation, DataOutput out, TypeInspector tx) {
        for (long typeid : typeids) {
            TitanType t = tx.getExistingType(typeid);
            writeInline(out, t, relation.getProperty(t), false);
        }
    }

    public Entry writeRelation(InternalRelation relation, int position, TypeInspector tx) {
        return writeRelation(relation, (InternalRelationType) relation.getType(),position,tx);
    }

    public Entry writeRelation(InternalRelation relation, InternalRelationType type, int position, TypeInspector tx) {
        assert type==relation.getType() || type.getBaseType().equals(relation.getType());
        Preconditions.checkArgument(position < relation.getLen());
        long typeid = type.getID();

        Direction dir = EdgeDirection.fromPosition(position);
        DirectionID dirID = getDirID(dir, relation.isProperty() ? RelationCategory.PROPERTY : RelationCategory.EDGE);

        DataOutput out = serializer.getDataOutput(DEFAULT_CAPACITY);
        int valuePosition;
        IDHandler.writeEdgeType(out, typeid, dirID, type.isHiddenRelationType());
        Multiplicity multiplicity = type.getMultiplicity();

        long[] sortKey = type.getSortKey();
        assert sortKey.length>0 ^ multiplicity.isConstrained();
        int keyStartPos = out.getPosition();
        if (!multiplicity.isConstrained()) {
            writeInlineTypes(sortKey, relation, out, tx);
        }
        int keyEndPos = out.getPosition();


        long relationIdDiff = relation.getID() - relation.getVertex(position).getID();
        //How multiplicity is handled for edges and properties is slightly different
        if (relation.isEdge()) {
            long vertexIdDiff = relation.getVertex((position + 1) % 2).getID() - relation.getVertex(position).getID();
            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) {
                    valuePosition = out.getPosition();
                    VariableLong.write(out, vertexIdDiff);
                } else {
                    VariableLong.write(out, vertexIdDiff);
                    valuePosition = out.getPosition();
                }
                VariableLong.write(out, relationIdDiff);
            } else {
                VariableLong.writeBackward(out, vertexIdDiff);
                VariableLong.writeBackward(out, relationIdDiff);
                valuePosition = out.getPosition();
            }
        } else {
            assert relation.isProperty();
            Preconditions.checkArgument(relation.isProperty());
            Object value = ((TitanProperty) relation).getValue();
            Preconditions.checkNotNull(value);
            TitanKey key = (TitanKey) type;
            assert key.getDataType().isInstance(value);

            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) { //Cardinality=SINGLE
                    valuePosition = out.getPosition();
                    writePropertyValue(out,key,value);
                } else { //Cardinality=SET
                    writePropertyValue(out,key,value);
                    valuePosition = out.getPosition();
                }
                VariableLong.write(out, relationIdDiff);
            } else {
                assert multiplicity.getCardinality()==Cardinality.LIST;
                VariableLong.writeBackward(out, relationIdDiff);
                valuePosition = out.getPosition();
                writePropertyValue(out,key,value);
            }
        }

        //Write signature
        long[] signature = type.getSignature();
        writeInlineTypes(signature, relation, out, tx);

        //Write remaining properties
        LongSet writtenTypes = new LongOpenHashSet(sortKey.length + signature.length);
        if (sortKey.length > 0 || signature.length > 0) {
            for (long id : sortKey) writtenTypes.add(id);
            for (long id : signature) writtenTypes.add(id);
        }
        LongArrayList remainingTypes = new LongArrayList(8);
        for (TitanType t : relation.getPropertyKeysDirect()) {
            if (!writtenTypes.contains(t.getID())) {
                remainingTypes.add(t.getID());

            }
        }
        //Sort types before writing to ensure that value is always written the same way
        long[] remaining = remainingTypes.toArray();
        Arrays.sort(remaining);
        for (long tid : remaining) {
            TitanType t = tx.getExistingType(tid);
            writeInline(out, t, relation.getProperty(t), true);
        }
        assert valuePosition>0;

        return new StaticArrayEntry(type.getSortOrder()==Order.DESC?
                                    out.getStaticBufferFlipBytes(keyStartPos,keyEndPos):
                                    out.getStaticBuffer(),valuePosition);
    }

    private void writeInline(DataOutput out, TitanType type, Object value, boolean writeEdgeType) {
        Preconditions.checkArgument(!(type.isPropertyKey() && !writeEdgeType) || !AttributeUtil.hasGenericDataType((TitanKey) type));

        if (writeEdgeType) {
            IDHandler.writeInlineEdgeType(out, type.getID());
        }

        if (type.isPropertyKey()) {
            writePropertyValue(out,(TitanKey)type,value);
        } else {
            assert type.isEdgeLabel();
            Preconditions.checkArgument(((TitanLabel) type).isUnidirected());
            if (value == null) {
                VariableLong.writePositive(out, 0);
            } else {
                VariableLong.writePositive(out, ((InternalVertex) value).getID());
            }
        }
    }

    private void writePropertyValue(DataOutput out, TitanKey key, Object value) {
        if (AttributeUtil.hasGenericDataType(key)) {
            out.writeClassAndObject(value);
        } else {
            out.writeObject(value, key.getDataType());
        }
    }

    public SliceQuery getQuery(RelationCategory resultType) {
        Preconditions.checkNotNull(resultType);
        StaticBuffer[] bound = getBounds(resultType);
        return new SliceQuery(bound[0], bound[1]);
    }

    public SliceQuery getQuery(InternalRelationType type, Direction dir, TypedInterval[] sortKey, VertexConstraint vertexCon) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(dir);

        StaticBuffer sliceStart = null, sliceEnd = null;
        RelationCategory rt = type.isPropertyKey() ? RelationCategory.PROPERTY : RelationCategory.EDGE;
        if (dir == Direction.BOTH) {
            sliceStart = IDHandler.getEdgeType(type.getID(), getDirID(Direction.OUT, rt),type.isHiddenRelationType());
            sliceEnd = IDHandler.getEdgeType(type.getID(), getDirID(Direction.IN, rt),type.isHiddenRelationType());
            assert sliceStart.compareTo(sliceEnd)<0;
            sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
        } else {
            DirectionID dirID = getDirID(dir, rt);

            DataOutput colStart = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY);
            DataOutput colEnd = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY);
            IDHandler.writeEdgeType(colStart, type.getID(), dirID, type.isHiddenRelationType());
            IDHandler.writeEdgeType(colEnd, type.getID(), dirID, type.isHiddenRelationType());

            long[] sortKeyIDs = type.getSortKey();
            Preconditions.checkArgument(sortKey.length == sortKeyIDs.length);
            assert colStart.getPosition() == colEnd.getPosition();
            int startPosition = colStart.getPosition();
            int i;
            boolean wroteInterval = false;
            Preconditions.checkArgument(sortKey.length>0 ^ type.getMultiplicity().isConstrained(),"Cannot use sort key with constrained types");
            for (i = 0; i < sortKeyIDs.length && sortKey[i] != null; i++) {
                TitanType t = sortKey[i].type;
                Interval interval = sortKey[i].interval;
                if (interval == null || interval.isEmpty()) {
                    break;
                }
                Preconditions.checkArgument(t.getID() == sortKeyIDs[i]);
                if (interval.isPoint()) {
                    writeInline(colStart, t, interval.getStart(), false);
                    writeInline(colEnd, t, interval.getEnd(), false);
                } else {
                    if (interval.getStart() != null)
                        writeInline(colStart, t, interval.getStart(), false);
                    if (interval.getEnd() != null)
                        writeInline(colEnd, t, interval.getEnd(), false);

                    switch (type.getSortOrder()) {
                        case ASC:
                            sliceStart = colStart.getStaticBuffer();
                            sliceEnd = colEnd.getStaticBuffer();
                            if (!interval.startInclusive()) sliceStart = BufferUtil.nextBiggerBuffer(sliceStart);
                            if (interval.endInclusive()) sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
                            break;

                        case DESC:
                            sliceEnd = colStart.getStaticBufferFlipBytes(startPosition,colStart.getPosition());
                            sliceStart = colEnd.getStaticBufferFlipBytes(startPosition,colEnd.getPosition());
                            if (interval.startInclusive()) sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
                            if (!interval.endInclusive()) sliceStart = BufferUtil.nextBiggerBuffer(sliceStart);
                            break;

                        default: throw new AssertionError(type.getSortOrder().toString());
                    }

                    assert sliceStart.compareTo(sliceEnd)<=0;
                    wroteInterval = true;
                    break;
                }
            }
            boolean wroteEntireSortKey = (i >= sortKeyIDs.length);
            assert !wroteEntireSortKey || !wroteInterval;
            assert !wroteInterval || vertexCon == null;

            if (!wroteInterval) {
                assert (colStart.getPosition() == colEnd.getPosition());
                int endPosition = colStart.getPosition();

                if (vertexCon != null) {
                    Preconditions.checkArgument(wroteEntireSortKey && !type.getMultiplicity().isUnique(dir));
                    Preconditions.checkArgument(type.isEdgeLabel());
                    long vertexIdDiff = vertexCon.getVertexIdDiff();
                    if (type.getMultiplicity().isConstrained())
                        VariableLong.write(colStart,vertexIdDiff);
                    else
                        VariableLong.writeBackward(colStart, vertexIdDiff);
                }

                switch (type.getSortOrder()) {
                    case ASC:
                        sliceStart = colStart.getStaticBuffer();
                        break;

                    case DESC:
                        sliceStart = colStart.getStaticBufferFlipBytes(startPosition,endPosition);
                        break;

                    default: throw new AssertionError(type.getSortOrder().toString());
                }
                sliceEnd = BufferUtil.nextBiggerBuffer(sliceStart);
            }
        }
        return new SliceQuery(sliceStart, sliceEnd);
    }

    public static class VertexConstraint {
        public final long vertexID;
        public final long otherVertexID;

        public VertexConstraint(long vertexID, long otherVertexID) {
            this.vertexID = vertexID;
            this.otherVertexID = otherVertexID;
        }

        private long getVertexIdDiff() {
            return otherVertexID - vertexID;
        }
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
