package com.thinkaurelius.faunus;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ListMultimap;
import com.thinkaurelius.faunus.FaunusElement.MicroElement;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.ReadByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusSerializer {

    public static final FaunusSerializer DEFAULT_SERIALIZER = new FaunusSerializer(new KryoSerializer(true),FaunusType.DEFAULT_MANAGER);

    private final Serializer serializer;
    private final FaunusType.Manager types;

    public FaunusSerializer(Serializer serializer, FaunusType.Manager typeManager) {
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(typeManager);
        this.serializer = serializer;
        this.types = typeManager;
    }

    public void writeVertex(final FaunusVertex vertex, final DataOutput out) throws IOException {
        Schema schema = new Schema();
        vertex.updateSchema(schema);
        schema.writeSchema(out);
        writeElement(vertex, schema, out);
        writeEdges(vertex.inEdges, out, Direction.OUT, schema);
        writeEdges(vertex.outEdges, out, Direction.IN, schema);
    }

    public void readVertex(final FaunusVertex vertex, final DataInput in) throws IOException {
        Schema schema = readSchema(in);
        readElement(vertex, schema, in);
        vertex.inEdges = readEdges(in, Direction.OUT, vertex.id, schema);
        vertex.outEdges = readEdges(in, Direction.IN, vertex.id, schema);
    }

    public void writeEdge(final FaunusEdge edge, final DataOutput out) throws IOException {
        writeElement(edge,out);
        WritableUtils.writeVLong(out, edge.inVertex);
        WritableUtils.writeVLong(out, edge.outVertex);
        writeFaunusType(edge.getType(), out);
    }

    public void readEdge(final FaunusEdge edge, final DataInput in) throws IOException {
        readElement(edge,in);
        edge.inVertex = WritableUtils.readVLong(in);
        edge.outVertex = WritableUtils.readVLong(in);
        edge.setLabel(readFaunusType(in));
    }

    public void readElement(final FaunusElement element, final DataInput in) throws IOException {
        readElement(element, null, in);
    }

    public void writeElement(final FaunusElement element, final DataOutput out) throws IOException {
        writeElement(element, null, out);
    }

    public void readElement(final FaunusElement element, final Schema schema, final DataInput in) throws IOException {
        element.id = WritableUtils.readVLong(in);
        element.pathEnabled = in.readBoolean();
        if (element.pathEnabled) {
            element.paths = readElementPaths(in);
            element.microVersion = (element instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(element.id) : new FaunusEdge.MicroEdge(element.id);
        } else
            element.pathCounter = WritableUtils.readVLong(in);
        element.properties = readProperties(schema, in);
    }

    private void writeElement(final FaunusElement element, final Schema schema, final DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, element.id);
        out.writeBoolean(element.pathEnabled);
        if (element.pathEnabled)
            writeElementPaths(element.paths, out);
        else
            WritableUtils.writeVLong(out, element.pathCounter);
        writeProperties(element.properties, schema, out);
    }

    private void writeProperties(final ListMultimap<FaunusType, Object> properties, final Schema schema, final DataOutput out) throws IOException {
        if (properties.isEmpty())
            WritableUtils.writeVInt(out, 0);
        else {
//            WritableUtils.writeVInt(out, properties.size());
            final com.thinkaurelius.titan.graphdb.database.serialize.DataOutput o = serializer.getDataOutput(properties.size()*40, true);
            for (final Map.Entry<FaunusType, Object> entry : properties.entries()) {
                if (schema==null) writeFaunusType(entry.getKey(),o);
                else VariableLong.writePositive(o,schema.getTypeId(entry.getKey()));
                o.writeClassAndObject(entry.getValue());
            }
            final StaticBuffer buffer = o.getStaticBuffer();
            WritableUtils.writeVInt(out, buffer.length());
            out.write(ByteBufferUtil.getArray(buffer.asByteBuffer()));
        }
    }

    private ListMultimap<FaunusType, Object> readProperties(final Schema schema, final DataInput in) throws IOException {
        final int numPropertyBytes = WritableUtils.readVInt(in);
        if (numPropertyBytes == 0)
            return null;
        else {
            final ListMultimap<FaunusType, Object> properties = ArrayListMultimap.create();
//            byte[] bytes = new byte[WritableUtils.readVInt(in)];
            byte[] bytes = new byte[numPropertyBytes];
            in.readFully(bytes);
            final ReadBuffer buffer = new ReadByteBuffer(bytes);
            while (buffer.hasRemaining()) {
                FaunusType type;
                if (schema==null) type = readFaunusType(buffer);
                else type = schema.getType(VariableLong.readPositive(buffer));
                Object value = serializer.readClassAndObject(buffer);
                properties.put(type,value);
            }
            return properties;
        }
    }

    public ListMultimap<FaunusType, FaunusEdge> readEdges(final DataInput in, final Direction idToRead, final long otherId, final Schema schema) throws IOException {
        final ListMultimap<FaunusType,FaunusEdge> edges = ArrayListMultimap.create();
        int edgeTypes = WritableUtils.readVInt(in);
        for (int i = 0; i < edgeTypes; i++) {
            FaunusType type = schema.getType(WritableUtils.readVLong(in));
            final int size = WritableUtils.readVInt(in);
            for (int j = 0; j < size; j++) {
                final FaunusEdge edge = new FaunusEdge();
                readElement(edge,schema,in);
                edge.setLabel(type);
                long vertexId = WritableUtils.readVLong(in);
                switch(idToRead) {
                    case IN:
                        edge.inVertex = vertexId;
                        edge.outVertex = otherId;
                        break;
                    case OUT:
                        edge.outVertex = vertexId;
                        edge.inVertex = otherId;
                        break;
                    default: throw ExceptionFactory.bothIsNotSupported();
                }
                edges.put(type, edge);
            }
        }
        return edges;
    }

    public void writeEdges(final ListMultimap<FaunusType, FaunusEdge> edges, final DataOutput out, final Direction idToWrite, final Schema schema) throws IOException {
        WritableUtils.writeVInt(out, edges.keySet().size());
        for (FaunusType type : edges.keySet()) {
            List<FaunusEdge> subset = edges.get(type);
            WritableUtils.writeVLong(out, schema.getTypeId(type));
            WritableUtils.writeVInt(out, subset.size());
            for (final FaunusEdge edge : subset) {
                writeElement(edge,schema,out);
                WritableUtils.writeVLong(out, edge.getVertexId(idToWrite));
            }
        }
    }

    private void writeElementPaths(final List<List<MicroElement>> paths, final DataOutput out) throws IOException {
        if (null == paths) {
            WritableUtils.writeVInt(out, 0);
        } else {
            WritableUtils.writeVInt(out, paths.size());
            for (final List<MicroElement> path : paths) {
                WritableUtils.writeVInt(out, path.size());
                for (MicroElement element : path) {
                    if (element instanceof FaunusVertex.MicroVertex)
                        out.writeChar('v');
                    else
                        out.writeChar('e');
                    WritableUtils.writeVLong(out, element.getId());
                }
            }
        }
    }

    private List<List<MicroElement>> readElementPaths(final DataInput in) throws IOException {
        int pathsSize = WritableUtils.readVInt(in);
        if (pathsSize == 0)
            return new ArrayList<List<MicroElement>>();
        else {
            final List<List<MicroElement>> paths = new ArrayList<List<MicroElement>>(pathsSize);
            for (int i = 0; i < pathsSize; i++) {
                int pathSize = WritableUtils.readVInt(in);
                final List<MicroElement> path = new ArrayList<MicroElement>(pathSize);
                for (int j = 0; j < pathSize; j++) {
                    char type = in.readChar();
                    if (type == 'v')
                        path.add(new FaunusVertex.MicroVertex(WritableUtils.readVLong(in)));
                    else
                        path.add(new FaunusEdge.MicroEdge(WritableUtils.readVLong(in)));
                }
                paths.add(path);
            }
            return paths;
        }
    }

    private void writeFaunusType(final FaunusType type, final DataOutput out) throws IOException {
        out.writeUTF(type.getName());
    }

    private FaunusType readFaunusType(final DataInput in) throws IOException {
        return types.get(in.readUTF());
    }

    private void writeFaunusType(final FaunusType type, final com.thinkaurelius.titan.graphdb.database.serialize.DataOutput out) throws IOException {
        out.writeObjectNotNull(type.getName());
    }

    private FaunusType readFaunusType(final ReadBuffer buffer) throws IOException {
        return types.get(serializer.readObjectNotNull(buffer, String.class));
    }

    class Schema {

        private final BiMap<FaunusType,Long> types;
        private long count = 1;

        private Schema() {
            this(8);
        }

        private Schema(int size) {
            types = HashBiMap.create(size);
        }

        void add(FaunusType type) {
            if (!types.containsKey(type)) types.put(type,count++);
        }

        void addAll(Iterable<FaunusType> types) {
            for (FaunusType type : types) add(type);
        }

        long getTypeId(FaunusType type) {
            Long id = types.get(type);
            Preconditions.checkArgument(id!=null,"Type is not part of the schema: " + type);
            return id;
        }

        FaunusType getType(long id) {
            FaunusType type = types.inverse().get(id);
            Preconditions.checkArgument(type!=null,"Type is not part of the schema: " + id);
            return type;
        }

        private void add(FaunusType type, long index) {
            Preconditions.checkArgument(!types.containsValue(index));
            types.put(type,index);
            count = index+1;
        }

        private void writeSchema(final DataOutput out) throws IOException  {
            WritableUtils.writeVInt(out,types.size());
            for (Map.Entry<FaunusType,Long> entry : types.entrySet()) {
                writeFaunusType(entry.getKey(),out);
                WritableUtils.writeVLong(out, entry.getValue());
            }
        }

    }

    private Schema readSchema(final DataInput in) throws IOException {
        int size = WritableUtils.readVInt(in);
        Schema schema = new Schema(size);
        for (int i=0;i<size;i++) {
            schema.add(readFaunusType(in),WritableUtils.readVLong(in));
        }
        return schema;
    }



    static {
        WritableComparator.define(FaunusElement.class, new Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FaunusElement.class);
        }

        @Override
        public int compare(final byte[] element1, final int start1, final int length1, final byte[] element2, final int start2, final int length2) {
            try {
                return Long.valueOf(readVLong(element1, start1)).compareTo(readVLong(element2, start2));
            } catch (IOException e) {
                return -1;
            }
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            if (a instanceof FaunusElement && b instanceof FaunusElement)
                return ((Long) (((FaunusElement) a).getIdAsLong())).compareTo(((FaunusElement) b).getIdAsLong());
            else
                return super.compare(a, b);
        }
    }

}
