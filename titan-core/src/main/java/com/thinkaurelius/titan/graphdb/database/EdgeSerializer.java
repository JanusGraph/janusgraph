package com.thinkaurelius.titan.graphdb.database;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanType;
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
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.relations.CacheEdge;
import com.thinkaurelius.titan.graphdb.relations.CacheProperty;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.ImmutableLongObjectMap;
import com.thinkaurelius.titan.util.datastructures.Interval;
import com.tinkerpop.blueprints.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class EdgeSerializer {

    private static final Logger log = LoggerFactory.getLogger(EdgeSerializer.class);


    private static final int DEFAULT_COLUMN_CAPACITY = 60;
    private static final int DEFAULT_VALUE_CAPACITY = 128;

    private static final long DIRECTION_ID = -101;
    private static final long TYPE_ID = -102;
    private static final long VALUE_ID = -103;
    private static final long OTHER_VERTEX_ID = -104;
    private static final long RELATION_ID = -105;

    private final Serializer serializer;
    private final IDManager idManager;

    public EdgeSerializer(Serializer serializer, IDManager idManager) {
        this.serializer = serializer;
        this.idManager = idManager;
    }

    public InternalRelation readRelation(InternalVertex vertex, Entry data) {
        StandardTitanTx tx = vertex.tx();
        ImmutableLongObjectMap map = getProperties(vertex.getID(), data, true, tx);

        Direction dir = (Direction) map.get(DIRECTION_ID);
        long typeid = (Long) map.get(TYPE_ID);
        TitanType type = tx.getExistingType(typeid);
        long relationId = (Long) map.get(RELATION_ID);
        if (type.isPropertyKey()) {
            Preconditions.checkArgument(dir == Direction.OUT);
            Object value = map.get(VALUE_ID);
            return new CacheProperty(relationId, (TitanKey) type, vertex, value, data);
        } else if (type.isEdgeLabel()) {
            long otherid = (Long) map.get(OTHER_VERTEX_ID);
            InternalVertex otherv = tx.getExistingVertex(otherid);
            if (dir == Direction.IN) {
                return new CacheEdge(relationId, (TitanLabel) type, otherv, vertex, (byte) 1, data);
            } else if (dir == Direction.OUT) {
                return new CacheEdge(relationId, (TitanLabel) type, vertex, otherv, (byte) 0, data);
            } else throw new AssertionError();
        } else throw new AssertionError();
    }

    public void readRelation(RelationFactory factory, Entry data, StandardTitanTx tx) {
        ImmutableLongObjectMap map = getProperties(factory.getVertexID(), data, false, tx);

        factory.setDirection((Direction) map.get(DIRECTION_ID));
        long typeid = (Long) map.get(TYPE_ID);
        TitanType type = tx.getExistingType(typeid);
        factory.setType(type);
        factory.setRelationID((Long) map.get(RELATION_ID));
        if (type.isPropertyKey()) {
            factory.setValue(map.get(VALUE_ID));
        } else if (type.isEdgeLabel()) {
            factory.setOtherVertexID((Long) map.get(OTHER_VERTEX_ID));
        } else throw new AssertionError();
        //Add properties
        for (int i = 0; i < map.size(); i++) {
            long propTypeId = map.getKey(i);
            if (propTypeId > 0) {
                TitanType pt = tx.getExistingType(propTypeId);
                if (map.getValue(i) != null) {
                    factory.addProperty(pt, map.getValue(i));
                }
            }
        }
    }

    public ImmutableLongObjectMap readProperties(InternalVertex vertex, Entry data, StandardTitanTx tx) {
        return getProperties(vertex.getID(), data, false, tx);
    }

    public ImmutableLongObjectMap getProperties(long vertexid, Entry data, boolean parseHeaderOnly, StandardTitanTx tx) {
        ImmutableLongObjectMap map = data.getCache();
        if (map == null) {
//                synchronized (data) {
//                    if (data.getCache()==null) {
            map = parseProperties(vertexid, data, parseHeaderOnly, tx);
            if (!parseHeaderOnly) data.setCache(map);
//                    } else map = data.getCache();
//                }
        }
        return map;
    }


    public Direction parseDirection(Entry data) {
        long[] typeAndDir = IDHandler.readEdgeType(data.getReadColumn());
        int dirID = (int) typeAndDir[1];

        switch (dirID) {
            case PROPERTY_DIR:
            case EDGE_OUT_DIR:
                return Direction.OUT;
            case EDGE_IN_DIR:
                return Direction.IN;
            default:
                throw new IllegalArgumentException("Invalid dirID read from disk: " + dirID);
        }
    }

    private ImmutableLongObjectMap parseProperties(long vertexid, Entry data, boolean parseHeaderOnly, StandardTitanTx tx) {
        Preconditions.checkArgument(vertexid > 0);
        ImmutableLongObjectMap.Builder builder = new ImmutableLongObjectMap.Builder();

        ReadBuffer column = data.getReadColumn();
        ReadBuffer value = data.getReadValue();

        long[] typeAndDir = IDHandler.readEdgeType(column);
        int dirID = (int) typeAndDir[1];
        long typeId = typeAndDir[0];

        Direction dir = null;
        RelationType rtype = null;
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
        builder.put(DIRECTION_ID, dir);
        builder.put(TYPE_ID, typeId);
        TitanType titanType = tx.getExistingType(typeId);

        InternalType def = (InternalType) titanType;
        long[] keysig = def.getSortKey();
        if (!parseHeaderOnly && !titanType.isUnique(dir)) {
            readInlineTypes(keysig, builder, column, tx);
        }

        long relationIdDiff, vertexIdDiff = 0;
        if (titanType.isUnique(dir)) {
            if (rtype == RelationType.EDGE) vertexIdDiff = VariableLong.read(value);
            relationIdDiff = VariableLong.read(value);
        } else {
            //Move position to end to read backwards
            column.movePosition(column.length() - column.getPosition() - 1);

            relationIdDiff = VariableLong.readBackward(column);
            if (rtype == RelationType.EDGE) vertexIdDiff = VariableLong.readBackward(column);
        }
        Preconditions.checkArgument(relationIdDiff + vertexid > 0);
        builder.put(RELATION_ID, relationIdDiff + vertexid);

        if (rtype == RelationType.EDGE) {
            Preconditions.checkArgument(titanType.isEdgeLabel());
            builder.put(OTHER_VERTEX_ID, vertexid + vertexIdDiff);
        }

        if (rtype == RelationType.PROPERTY) {
            Preconditions.checkArgument(titanType.isPropertyKey());
            TitanKey key = ((TitanKey) titanType);
            Object attribute = null;

            if (hasGenericDataType(key)) {
                attribute = serializer.readClassAndObject(value);
            } else {
                attribute = serializer.readObjectNotNull(value, key.getDataType());
            }
            Preconditions.checkNotNull(attribute);
            builder.put(VALUE_ID, attribute);
        }

        if (!parseHeaderOnly) {
            //value signature & sort key if unique
            if (titanType.isUnique(dir)) {
                readInlineTypes(keysig, builder, value, tx);
            }
            readInlineTypes(def.getSignature(), builder, value, tx);

            //Third: read rest
            while (value.hasRemaining()) {
                TitanType type = tx.getExistingType(IDHandler.readInlineEdgeType(value));
                builder.put(type.getID(), readInline(value, type));
            }
        }

        return builder.build();
    }

    private void readInlineTypes(long[] typeids, ImmutableLongObjectMap.Builder builder, ReadBuffer in, StandardTitanTx tx) {
        for (int i = 0; i < typeids.length; i++) {
            TitanType keyType = tx.getExistingType(typeids[i]);
            builder.put(typeids[i], readInline(in, keyType));
        }
    }

    private Object readInline(ReadBuffer read, TitanType type) {
        if (type.isPropertyKey()) {
            TitanKey proptype = ((TitanKey) type);
            if (hasGenericDataType(proptype))
                return serializer.readClassAndObject(read);
            else return serializer.readObject(read, proptype.getDataType());
        } else {
            assert type.isEdgeLabel();
            Long id = Long.valueOf(VariableLong.readPositive(read));
            if (id.longValue() == 0) return null;
            else return id;
        }
    }

    private static final boolean hasGenericDataType(TitanKey key) {
        return key.getDataType().equals(Object.class);
    }

    private static final int getDirID(Direction dir, RelationType rt) {
        if (rt == RelationType.PROPERTY) {
            Preconditions.checkArgument(dir == Direction.OUT);
            return PROPERTY_DIR;
        } else if (rt == RelationType.EDGE) {
            if (dir == Direction.OUT) return EDGE_OUT_DIR;
            else if (dir == Direction.IN) return EDGE_IN_DIR;
            else throw new IllegalArgumentException("Invalid direction: " + dir);
        } else {
            throw new IllegalArgumentException("Invalid relation type: " + rt);
        }

    }

    public Entry writeRelation(InternalRelation relation, int pos, StandardTitanTx tx) {
        return writeRelation(relation, pos, true, tx);
    }

    private void writeInlineTypes(long[] typeids, InternalRelation relation, DataOutput out, StandardTitanTx tx) {
        for (int i = 0; i < typeids.length; i++) {
            TitanType t = tx.getExistingType(typeids[i]);
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
        if (!type.isUnique(dir)) {
            writeInlineTypes(sortKey, relation, colOut, tx);
        }


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

        return new StaticBufferEntry(colOut.getStaticBuffer(), writer.getStaticBuffer());
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


//    private static int[] getDirIDInterval(Direction dir, RelationType rt) {
//        if (dir==Direction.OUT) {
//            if (rt== RelationType.PROPERTY) return new int[]{PROPERTY_DIR,PROPERTY_DIR};
//            else if (rt== RelationType.EDGE) return new int[]{EDGE_OUT_DIR,EDGE_OUT_DIR};
//            else if (rt== RelationType.RELATION) return new int[]{PROPERTY_DIR,EDGE_OUT_DIR};
//            else throw new IllegalArgumentException("Invalid dir+return-type: " + dir + "|" + rt);
//        } else if (dir==Direction.IN) {
//            if (rt== RelationType.EDGE) return new int[]{EDGE_IN_DIR,EDGE_IN_DIR};
//            else throw new IllegalArgumentException("Invalid dir+return-type: " + dir + "|" + rt);
//        } else if (dir==Direction.BOTH) {
//            if (rt== RelationType.PROPERTY) return new int[]{PROPERTY_DIR,PROPERTY_DIR};
//            else if (rt== RelationType.EDGE) return new int[]{EDGE_OUT_DIR,EDGE_IN_DIR};
//            else if (rt== RelationType.RELATION) return new int[]{PROPERTY_DIR,EDGE_IN_DIR};
//            else throw new IllegalArgumentException("Invalid dir+return-type: " + dir + "|" + rt);
//        } else throw new IllegalArgumentException("Invalid dir+return-type: " + dir + "|" + rt);
//    }
//
//    private static int getDirID(Direction dir, RelationType rt) {
//        int[] ids = getDirIDInterval(dir,rt);
//        Preconditions.checkArgument(ids[0]==ids[1],"Invalid arguments [%s] [%s]",dir,rt);
//        return ids[0];
//    }

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
            Preconditions.checkArgument(ByteBufferUtil.isSmallerThan(sliceStart, sliceEnd));
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

                    sliceStart = colStart.getStaticBuffer();
                    if (!interval.startInclusive()) sliceStart = ByteBufferUtil.nextBiggerBuffer(sliceStart);
                    sliceEnd = colEnd.getStaticBuffer();
                    if (interval.endInclusive()) sliceEnd = ByteBufferUtil.nextBiggerBuffer(sliceEnd);
                    wroteInterval = true;
                    break;
                }
            }
            boolean wroteEntireSortKey = (i >= sortKeyIDs.length);


            if (vertexCon != null) {
                Preconditions.checkArgument(wroteEntireSortKey && !type.isUnique(dir));
                Preconditions.checkArgument(type.isEdgeLabel());
                long vertexIdDiff = vertexCon.getVertexIdDiff();
                VariableLong.writeBackward(colStart, vertexIdDiff);
                VariableLong.writeBackward(colEnd, vertexIdDiff);

                //VariableLong.writeBackward(colStart,relationIdDiff);
            }
            if (!wroteInterval) {
                sliceStart = colStart.getStaticBuffer();
                sliceEnd = ByteBufferUtil.nextBiggerBuffer(colEnd.getStaticBuffer());
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

        private final long getVertexIdDiff() {
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
