package com.thinkaurelius.titan.graphdb.database;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.relations.CacheEdge;
import com.thinkaurelius.titan.graphdb.relations.CacheProperty;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.util.datastructures.Interval;
import com.tinkerpop.blueprints.Direction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class EdgeSerializer {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(EdgeSerializer.class);


    private static final int DEFAULT_COLUMN_CAPACITY = 60;
    private static final int DEFAULT_VALUE_CAPACITY = 128;

    private static final long DIRECTION_ID = -101;
    private static final long TYPE_ID = -102;
    private static final long VALUE_ID = -103;
    private static final long OTHER_VERTEX_ID = -104;
    private static final long RELATION_ID = -105;

    private final Serializer serializer;

    public EdgeSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public InternalRelation readRelation(final InternalVertex vertex, final Entry data) {
        StandardTitanTx tx = vertex.tx();
        RelationCache relation = readRelation(vertex.getID(), data, true, tx);

        TitanType type = tx.getExistingType(relation.typeId);

        if (type.isPropertyKey()) {
            assert relation.direction == Direction.OUT;
            return new CacheProperty(relation.relationId, (TitanKey) type, vertex, relation.getValue(), data);
        }

        if (type.isEdgeLabel()) {
            InternalVertex otherVertex = tx.getExistingVertex(relation.getOtherVertexId());
            switch (relation.direction) {
                case IN:
                    return new CacheEdge(relation.relationId, (TitanLabel) type, otherVertex, vertex, (byte) 1, data);

                case OUT:
                    return new CacheEdge(relation.relationId, (TitanLabel) type, vertex, otherVertex, (byte) 0, data);

                default:
                    throw new AssertionError();
            }
        }

        throw new AssertionError();
    }

    //Used by Faunus - DON'T REMOVE
    public void readRelation(RelationFactory factory, Entry data, StandardTitanTx tx) {
        RelationCache relation = readRelation(factory.getVertexID(), data, false, tx);
        assert relation.hasProperties();

        factory.setDirection(relation.direction);
        TitanType type = tx.getExistingType(relation.typeId);
        factory.setType(type);
        factory.setRelationID(relation.relationId);
        if (type.isPropertyKey()) {
            factory.setValue(relation.getValue());
        } else if (type.isEdgeLabel()) {
            factory.setOtherVertexID(relation.getOtherVertexId());
        } else throw new AssertionError();
        //Add properties
        for (LongObjectCursor<Object> entry : relation) {
            TitanType pt = tx.getExistingType(entry.key);
            if (entry.value != null) {
                factory.addProperty(pt, entry.value);
            }
        }
    }

    public RelationCache readRelation(InternalVertex vertex, Entry data, StandardTitanTx tx) {
        return readRelation(vertex.getID(), data, false, tx);
    }

    public RelationCache readRelation(long vertexid, Entry data, boolean parseHeaderOnly, StandardTitanTx tx) {
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

        long[] typeAndDir = IDHandler.readEdgeType(data.getReadColumn());
        switch ((int) typeAndDir[1]) {
            case PROPERTY_DIR:
            case EDGE_OUT_DIR:
                return Direction.OUT;
            case EDGE_IN_DIR:
                return Direction.IN;
            default:
                throw new IllegalArgumentException("Invalid dirID read from disk: " + typeAndDir[1]);
        }
    }

    private RelationCache parseRelation(long vertexid, Entry data, boolean parseHeaderOnly, StandardTitanTx tx) {
        assert vertexid > 0;

        ReadBuffer column = data.getReadColumn();
        ReadBuffer value = data.getReadValue();

        LongObjectOpenHashMap properties = parseHeaderOnly ? null : new LongObjectOpenHashMap(4);
        long[] typeAndDir = IDHandler.readEdgeType(column);
        int dirID = (int) typeAndDir[1];
        long typeId = typeAndDir[0];

        Direction dir;
        RelationType rtype;
        switch (dirID) {
            case PROPERTY_DIR:
                dir = Direction.OUT;
                rtype = RelationType.PROPERTY;
                break;

            case EDGE_OUT_DIR:
                dir = Direction.OUT;
                rtype = RelationType.EDGE;
                break;

            case EDGE_IN_DIR:
                dir = Direction.IN;
                rtype = RelationType.EDGE;
                break;

            default:
                throw new IllegalArgumentException("Invalid dirID read from disk: " + dirID);
        }

        TitanType titanType = tx.getExistingType(typeId);

        InternalType def = (InternalType) titanType;
        long[] keysig = def.getSortKey();
        if (!parseHeaderOnly && !titanType.isUnique(dir)) {
            ReadBuffer sortKeyReader = def.getSortOrder()==Order.DESC?column.invert():column;
            readInlineTypes(keysig, properties, sortKeyReader, tx);
        }

        long relationIdDiff, vertexIdDiff = 0;
        if (titanType.isUnique(dir)) {
            if (rtype == RelationType.EDGE)
                vertexIdDiff = VariableLong.read(value);
            relationIdDiff = VariableLong.read(value);
        } else {
            //Move position to end to read backwards
            column.movePosition(column.length() - column.getPosition() - 1);

            relationIdDiff = VariableLong.readBackward(column);
            if (rtype == RelationType.EDGE)
                vertexIdDiff = VariableLong.readBackward(column);
        }

        assert relationIdDiff + vertexid > 0;
        long relationId = relationIdDiff + vertexid;

        Object other;
        switch (rtype) {
            case EDGE:
                Preconditions.checkArgument(titanType.isEdgeLabel());
                other = vertexid + vertexIdDiff;
                break;

            case PROPERTY:
                Preconditions.checkArgument(titanType.isPropertyKey());
                TitanKey key = ((TitanKey) titanType);
                other = hasGenericDataType(key)
                        ? serializer.readClassAndObject(value)
                        : serializer.readObjectNotNull(value, key.getDataType());
                break;

            default:
                throw new AssertionError();
        }
        assert other != null;

        if (!parseHeaderOnly) {
            //value signature & sort key if unique
            if (titanType.isUnique(dir)) {
                readInlineTypes(keysig, properties, value, tx);
            }

            readInlineTypes(def.getSignature(), properties, value, tx);

            //Third: read rest
            while (value.hasRemaining()) {
                TitanType type = tx.getExistingType(IDHandler.readInlineEdgeType(value));
                Object pvalue = readInline(value, type);
                assert pvalue != null;
                properties.put(type.getID(), pvalue);
            }
        }
        return new RelationCache(dir, typeId, relationId, other, properties);
    }

    private void readInlineTypes(long[] typeids, LongObjectOpenHashMap properties, ReadBuffer in, StandardTitanTx tx) {
        for (long typeid : typeids) {
            TitanType keyType = tx.getExistingType(typeid);
            Object value = readInline(in, keyType);
            if (value != null) properties.put(typeid, value);
        }
    }

    private Object readInline(ReadBuffer read, TitanType type) {
        if (type.isPropertyKey()) {
            TitanKey proptype = ((TitanKey) type);
            return hasGenericDataType(proptype)
                    ? serializer.readClassAndObject(read)
                    : serializer.readObject(read, proptype.getDataType());
        }

        assert type.isEdgeLabel();
        long id = VariableLong.readPositive(read);
        return id == 0 ? null : id;
    }

    private static boolean hasGenericDataType(TitanKey key) {
        return key.getDataType().equals(Object.class);
    }

    private static int getDirID(Direction dir, RelationType rt) {
        switch (rt) {
            case PROPERTY:
                assert dir == Direction.OUT;
                return PROPERTY_DIR;

            case EDGE:
                switch (dir) {
                    case OUT:
                        return EDGE_OUT_DIR;

                    case IN:
                        return EDGE_IN_DIR;

                    default:
                        throw new IllegalArgumentException("Invalid direction: " + dir);
                }

            default:
                throw new IllegalArgumentException("Invalid relation type: " + rt);
        }
    }

    public Entry writeRelation(InternalRelation relation, int pos, StandardTitanTx tx) {
        return writeRelation(relation, pos, true, tx);
    }

    private void writeInlineTypes(long[] typeids, InternalRelation relation, DataOutput out, StandardTitanTx tx) {
        for (long typeid : typeids) {
            TitanType t = tx.getExistingType(typeid);
            writeInline(out, t, relation.getProperty(t), false);
        }
    }

    public Entry writeRelation(InternalRelation relation, int position, boolean writeValue, StandardTitanTx tx) {
        Preconditions.checkArgument(position < relation.getLen());
        TitanType type = relation.getType();
        long typeid = type.getID();

        Direction dir = EdgeDirection.fromPosition(position);
        int dirID = getDirID(dir, relation.isProperty() ? RelationType.PROPERTY : RelationType.EDGE);

        DataOutput colOut = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY, true);
        IDHandler.writeEdgeType(colOut, typeid, dirID);

        InternalType definition = (InternalType) type;
        long[] sortKey = definition.getSortKey();
        int startPosition = colOut.getPosition();
        if (!type.isUnique(dir)) {
            writeInlineTypes(sortKey, relation, colOut, tx);
        }
        int endPosition = colOut.getPosition();

        DataOutput writer = colOut;
        long vertexIdDiff = 0;
        long relationIdDiff = relation.getID() - relation.getVertex(position).getID();
        if (relation.isEdge())
            vertexIdDiff = relation.getVertex((position + 1) % 2).getID() - relation.getVertex(position).getID();

        if (type.isUnique(dir)) {
            if (!writeValue) return new StaticBufferEntry(colOut.getStaticBuffer(), null);
            writer = serializer.getDataOutput(DEFAULT_VALUE_CAPACITY, true);
            if (relation.isEdge()) VariableLong.write(writer, vertexIdDiff);
            VariableLong.write(writer, relationIdDiff);
        } else {
            if (relation.isEdge()) VariableLong.writeBackward(writer, vertexIdDiff);
            VariableLong.writeBackward(writer, relationIdDiff);
        }

        if (!type.isUnique(dir)) {
            if (!writeValue) return new StaticBufferEntry(colOut.getStaticBuffer(), null);
            writer = serializer.getDataOutput(DEFAULT_VALUE_CAPACITY, true);
        }

        if (relation.isProperty()) {
            Preconditions.checkArgument(relation.isProperty());
            Object value = ((TitanProperty) relation).getValue();
            Preconditions.checkNotNull(value);
            TitanKey key = (TitanKey) type;
            assert key.getDataType().isInstance(value);
            if (hasGenericDataType(key)) {
                writer.writeClassAndObject(value);
            } else {
                writer.writeObjectNotNull(value);
            }
        }

        //Write signature & sort key if unique
        if (type.isUnique(dir)) {
            writeInlineTypes(sortKey, relation, writer, tx);
        }
        long[] signature = definition.getSignature();
        writeInlineTypes(signature, relation, writer, tx);


        //Write remaining properties
        LongSet writtenTypes = new LongOpenHashSet(sortKey.length + signature.length);
        if (sortKey.length > 0 || signature.length > 0) {
            for (long id : sortKey) writtenTypes.add(id);
            for (long id : signature) writtenTypes.add(id);
        }
        for (TitanType t : relation.getPropertyKeysDirect()) {
            if (!writtenTypes.contains(t.getID())) {
                writeInline(writer, t, relation.getProperty(t), true);
            }
        }

        StaticBuffer column = ((InternalType)type).getSortOrder()==Order.DESC?
                                    colOut.getStaticBufferFlipBytes(startPosition,endPosition):
                                    colOut.getStaticBuffer();
        return new StaticBufferEntry(column, writer.getStaticBuffer());
    }

    private void writeInline(DataOutput out, TitanType type, Object value, boolean writeEdgeType) {
        Preconditions.checkArgument(!(type.isPropertyKey() && !writeEdgeType) || !hasGenericDataType((TitanKey) type));

        if (writeEdgeType) {
            IDHandler.writeInlineEdgeType(out, type.getID());
        }

        if (type.isPropertyKey()) {
            if (hasGenericDataType((TitanKey) type)) {
                out.writeClassAndObject(value);
            } else {
                out.writeObject(value, ((TitanKey) type).getDataType());
            }
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

    public SliceQuery getQuery(RelationType resultType) {
        Preconditions.checkNotNull(resultType);
        StaticBuffer[] bound = getBounds(resultType);
        return new SliceQuery(bound[0], bound[1]);
    }

    public SliceQuery getQuery(InternalType type, Direction dir, TypedInterval[] sortKey, VertexConstraint vertexCon) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(dir);

        StaticBuffer sliceStart = null, sliceEnd = null;
        boolean isStatic;
        RelationType rt = type.isPropertyKey() ? RelationType.PROPERTY : RelationType.EDGE;
        if (dir == Direction.BOTH) {
            isStatic = type.isStatic(Direction.OUT) && type.isStatic(Direction.IN);
            sliceStart = IDHandler.getEdgeType(type.getID(), getDirID(Direction.OUT, rt));
            sliceEnd = IDHandler.getEdgeType(type.getID(), getDirID(Direction.IN, rt));
            assert ByteBufferUtil.isSmallerThan(sliceStart, sliceEnd);
            sliceEnd = ByteBufferUtil.nextBiggerBuffer(sliceEnd);
        } else {
            isStatic = type.isStatic(dir);
            int dirID = getDirID(dir, rt);

            DataOutput colStart = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY, true);
            DataOutput colEnd = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY, true);
            IDHandler.writeEdgeType(colStart, type.getID(), dirID);
            IDHandler.writeEdgeType(colEnd, type.getID(), dirID);

            long[] sortKeyIDs = type.getSortKey();
            Preconditions.checkArgument(sortKey.length == sortKeyIDs.length);
            assert colStart.getPosition() == colEnd.getPosition();
            int startPosition = colStart.getPosition();
            int i;
            boolean wroteInterval = false;
            for (i = 0; i < sortKeyIDs.length && sortKey[i] != null; i++) {
                TitanType t = sortKey[i].type;
                Interval interval = sortKey[i].interval;
                if (interval == null || interval.isEmpty()) {
                    break;
                }
                Preconditions.checkArgument(t.getID() == sortKeyIDs[i]);
                Preconditions.checkArgument(!type.isUnique(dir), "Cannot apply sort key to the unique direction");
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
                            if (!interval.startInclusive()) sliceStart = ByteBufferUtil.nextBiggerBuffer(sliceStart);
                            if (interval.endInclusive()) sliceEnd = ByteBufferUtil.nextBiggerBuffer(sliceEnd);
                            break;

                        case DESC:
                            sliceEnd = colStart.getStaticBufferFlipBytes(startPosition,colStart.getPosition());
                            sliceStart = colEnd.getStaticBufferFlipBytes(startPosition,colEnd.getPosition());
                            if (interval.startInclusive()) sliceEnd = ByteBufferUtil.nextBiggerBuffer(sliceEnd);
                            if (!interval.endInclusive()) sliceStart = ByteBufferUtil.nextBiggerBuffer(sliceStart);
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
                    assert !wroteInterval;
                    Preconditions.checkArgument(wroteEntireSortKey && !type.isUnique(dir));
                    Preconditions.checkArgument(type.isEdgeLabel());
                    long vertexIdDiff = vertexCon.getVertexIdDiff();
                    VariableLong.writeBackward(colStart, vertexIdDiff);

                    //VariableLong.writeBackward(colStart,relationIdDiff);
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
                sliceEnd = ByteBufferUtil.nextBiggerBuffer(sliceStart);
            }
        }
        return new SliceQuery(sliceStart, sliceEnd, isStatic);
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
        public final InternalType type;
        public final Interval interval;


        public TypedInterval(InternalType type, Interval interval) {
            this.type = type;
            this.interval = interval;
        }
    }

}
