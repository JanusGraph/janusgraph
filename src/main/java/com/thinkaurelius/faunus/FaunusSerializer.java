package com.thinkaurelius.faunus;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.thinkaurelius.faunus.FaunusPathElement.MicroElement;
import com.thinkaurelius.faunus.formats.rexster.util.ElementIdHandler;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ReadByteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusSerializer {

    private final Serializer serializer;
    private final FaunusType.Manager types;
    private final boolean trackElementState;
    private final boolean trackPaths;

    public FaunusSerializer(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        this.serializer = new KryoSerializer(true);
        this.types = FaunusType.DEFAULT_MANAGER;
        this.trackElementState = configuration.getBoolean(FaunusCompiler.ELEMENT_STATE, false);
        this.trackPaths = configuration.getBoolean(FaunusCompiler.PATH_ENABLED, false);
    }

    public void writeVertex(final FaunusVertex vertex, final DataOutput out) throws IOException {
        //Need to write the id up front for the comparator
        WritableUtils.writeVLong(out, vertex.id);
        Schema schema = new Schema();
        vertex.updateSchema(schema);
        schema.writeSchema(out);
        writePathElement(vertex, schema, out);
        writeEdges(vertex.inEdges, out, Direction.OUT, schema);
        writeEdges(vertex.outEdges, out, Direction.IN, schema);
    }

    public void readVertex(final FaunusVertex vertex, final DataInput in) throws IOException {
        WritableUtils.readVLong(in);
        Schema schema = readSchema(in);
        readPathElement(vertex, schema, in);
        vertex.inEdges = readEdges(in, Direction.OUT, vertex.id, schema);
        vertex.outEdges = readEdges(in, Direction.IN, vertex.id, schema);
    }

    public void writeEdge(final FaunusEdge edge, final DataOutput out) throws IOException {
        writePathElement(edge, out);
        WritableUtils.writeVLong(out, edge.inVertex);
        WritableUtils.writeVLong(out, edge.outVertex);
        writeFaunusType(edge.getType(), out);
    }

    public void readEdge(final FaunusEdge edge, final DataInput in) throws IOException {
        readPathElement(edge, in);
        edge.inVertex = WritableUtils.readVLong(in);
        edge.outVertex = WritableUtils.readVLong(in);
        edge.setLabel(readFaunusType(in));
    }

    private void readPathElement(final FaunusPathElement element, final DataInput in) throws IOException {
        readPathElement(element, null, in);
    }

    private void writePathElement(final FaunusPathElement element, final DataOutput out) throws IOException {
        writePathElement(element, null, out);
    }

    private void readPathElement(final FaunusPathElement element, Schema schema, final DataInput in) throws IOException {
        readElement(element, schema, in);
        if (trackPaths) {
            element.paths = readElementPaths(in);
            element.microVersion = (element instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(element.id) : new FaunusEdge.MicroEdge(element.id);
            element.pathEnabled = true;
        } else {
            element.pathCounter = WritableUtils.readVLong(in);
            element.pathEnabled = false;
        }
    }

    private void writePathElement(final FaunusPathElement element, final Schema schema, final DataOutput out) throws IOException {
        writeElement(element, schema, out);
        if (trackPaths)
            writeElementPaths(element.paths, out);
        else
            WritableUtils.writeVLong(out, element.pathCounter);
    }

    private void readElement(final FaunusElement element, Schema schema, final DataInput in) throws IOException {
        element.id = WritableUtils.readVLong(in);
        if (trackElementState) element.setState(ElementState.valueOf(in.readByte()));
        element.properties = readProperties(schema, in);
    }

    private void writeElement(final FaunusElement element, final Schema schema, final DataOutput out) throws IOException {
        Preconditions.checkArgument(trackElementState || !element.isDeleted());
        WritableUtils.writeVLong(out, element.id);
        if (trackElementState) out.writeByte(element.getState().getByteValue());
        writeProperties(element.properties, schema, out);
    }

    private <T extends FaunusElement> Iterable<T> filterDeleted(Iterable<T> elements) {
        if (trackElementState) return elements;
        else return Iterables.filter(elements, new Predicate<T>() {
            @Override
            public boolean apply(@Nullable T element) {
                return !element.isDeleted();
            }
        });
    }

    private void writeProperties(final Multimap<FaunusType, FaunusProperty> properties, final Schema schema, final DataOutput out) throws IOException {
        Iterable<FaunusProperty> subset = filterDeleted(properties.values());
        int count = IterablesUtil.size(subset);
        WritableUtils.writeVInt(out, count);
        if (count > 0) {
            for (final FaunusProperty property : subset) {
                //Type
                if (schema == null) writeFaunusType(property.getType(), out);
                else WritableUtils.writeVLong(out, schema.getTypeId(property.getType()));
                //Value
                final com.thinkaurelius.titan.graphdb.database.serialize.DataOutput o = serializer.getDataOutput(40, true);
                o.writeClassAndObject(property.getValue());
                final StaticBuffer buffer = o.getStaticBuffer();
                WritableUtils.writeVInt(out, buffer.length());
                out.write(buffer.as(StaticBuffer.ARRAY_FACTORY));
                //Element
                writeElement(property, schema, out);
            }
        }
    }

    private Multimap<FaunusType, FaunusProperty> readProperties(final Schema schema, final DataInput in) throws IOException {
        final int count = WritableUtils.readVInt(in);
        if (count == 0)
            return FaunusElement.NO_PROPERTIES;
        else {
            final Multimap<FaunusType, FaunusProperty> properties = HashMultimap.create();
            for (int i = 0; i < count; i++) {
                FaunusType type;
                if (schema == null) type = readFaunusType(in);
                else type = schema.getType(WritableUtils.readVLong(in));
                //Value
                int byteLength = WritableUtils.readVInt(in);
                byte[] bytes = new byte[byteLength];
                in.readFully(bytes);
                final ReadBuffer buffer = new ReadByteBuffer(bytes);
                Object value = serializer.readClassAndObject(buffer);

                FaunusProperty property = new FaunusProperty(type, value);
                readElement(property, schema, in);
                properties.put(type, property);
            }
            return properties;
        }
    }

    private ListMultimap<FaunusType, FaunusEdge> readEdges(final DataInput in, final Direction idToRead, final long otherId, final Schema schema) throws IOException {
        final ListMultimap<FaunusType, FaunusEdge> edges = ArrayListMultimap.create();
        int edgeTypes = WritableUtils.readVInt(in);
        for (int i = 0; i < edgeTypes; i++) {
            FaunusType type = schema.getType(WritableUtils.readVLong(in));
            final int size = WritableUtils.readVInt(in);
            for (int j = 0; j < size; j++) {
                final FaunusEdge edge = new FaunusEdge();
                readPathElement(edge, schema, in);
                edge.setLabel(type);
                long vertexId = WritableUtils.readVLong(in);
                switch (idToRead) {
                    case IN:
                        edge.inVertex = vertexId;
                        edge.outVertex = otherId;
                        break;
                    case OUT:
                        edge.outVertex = vertexId;
                        edge.inVertex = otherId;
                        break;
                    default:
                        throw ExceptionFactory.bothIsNotSupported();
                }
                edges.put(type, edge);
            }
        }
        return edges;
    }

    private void writeEdges(final ListMultimap<FaunusType, FaunusEdge> edges, final DataOutput out, final Direction idToWrite, final Schema schema) throws IOException {
        Map<FaunusType, Integer> counts = Maps.newHashMap();
        int typeCount = 0;
        for (FaunusType type : edges.keySet()) {
            int count = IterablesUtil.size(filterDeleted(edges.get(type)));
            counts.put(type, count);
            if (count > 0) typeCount++;
        }

        WritableUtils.writeVInt(out, typeCount);
        for (FaunusType type : edges.keySet()) {
            if (counts.get(type) == 0) continue;
            Iterable<FaunusEdge> subset = filterDeleted(edges.get(type));
            WritableUtils.writeVLong(out, schema.getTypeId(type));
            WritableUtils.writeVInt(out, counts.get(type));
            for (final FaunusEdge edge : subset) {
                writePathElement(edge, schema, out);
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


    class Schema {

        private final BiMap<FaunusType, Long> localTypes;
        private long count = 1;

        private Schema() {
            this(8);
        }

        private Schema(int size) {
            this.localTypes = HashBiMap.create(size);
        }

        void add(String type) {
            this.add(types.get(type));
        }

        void add(FaunusType type) {
            if (!localTypes.containsKey(type)) localTypes.put(type, count++);
        }

        void addAll(Iterable<FaunusType> types) {
            for (FaunusType type : types) add(type);
        }

        long getTypeId(FaunusType type) {
            Long id = localTypes.get(type);
            Preconditions.checkArgument(id != null, "Type is not part of the schema: " + type);
            return id;
        }

        FaunusType getType(long id) {
            FaunusType type = localTypes.inverse().get(id);
            Preconditions.checkArgument(type != null, "Type is not part of the schema: " + id);
            return type;
        }

        private void add(FaunusType type, long index) {
            Preconditions.checkArgument(!localTypes.containsValue(index));
            localTypes.put(type, index);
            count = index + 1;
        }

        private void writeSchema(final DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, localTypes.size());
            for (Map.Entry<FaunusType, Long> entry : localTypes.entrySet()) {
                writeFaunusType(entry.getKey(), out);
                WritableUtils.writeVLong(out, entry.getValue());
            }
        }

    }

    private Schema readSchema(final DataInput in) throws IOException {
        int size = WritableUtils.readVInt(in);
        Schema schema = new Schema(size);
        for (int i = 0; i < size; i++) {
            schema.add(readFaunusType(in), WritableUtils.readVLong(in));
        }
        return schema;
    }


    static {
        WritableComparator.define(FaunusPathElement.class, new Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FaunusPathElement.class);
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

    //################################################
    // Serialization for vanilla Blueprints
    //################################################

    public void writeVertex(final Vertex vertex, final ElementIdHandler elementIdHandler, final DataOutput out) throws IOException {
        Schema schema = new Schema();
        //Convert properties and update schema
        Multimap<FaunusType, FaunusProperty> properties = getProperties(vertex);
        for (FaunusType type : properties.keySet()) schema.add(type);
        for (Edge edge : vertex.getEdges(Direction.BOTH)) {
            schema.add(edge.getLabel());
            for (String key : edge.getPropertyKeys()) schema.add(key);
        }

        WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(vertex));
        schema.writeSchema(out);
        WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(vertex));
        if (trackElementState) out.writeByte(ElementState.NEW.getByteValue());
        writeProperties(properties, schema, out);
        out.writeBoolean(false);
        WritableUtils.writeVLong(out, 0);
        writeEdges(vertex, Direction.IN, elementIdHandler, schema, out);
        writeEdges(vertex, Direction.OUT, elementIdHandler, schema, out);

    }

    private Multimap<FaunusType, FaunusProperty> getProperties(Element element) {
        Multimap<FaunusType, FaunusProperty> properties = HashMultimap.create();
        for (String key : element.getPropertyKeys()) {
            FaunusType type = types.get(key);
            properties.put(type, new FaunusProperty(type, element.getProperty(key)));
        }
        return properties;
    }

    private void writeEdges(final Vertex vertex, final Direction direction, final ElementIdHandler elementIdHandler,
                            final Schema schema, final DataOutput out) throws IOException {
        final Multiset<String> labelCount = HashMultiset.create();
        for (final Edge edge : vertex.getEdges(direction)) {
            labelCount.add(edge.getLabel());
        }
        WritableUtils.writeVInt(out, labelCount.elementSet().size());
        for (String label : labelCount.elementSet()) {
            FaunusType type = types.get(label);
            WritableUtils.writeVLong(out, schema.getTypeId(type));
            WritableUtils.writeVInt(out, labelCount.count(label));
            for (final Edge edge : vertex.getEdges(direction, label)) {
                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge));
                if (trackElementState) out.writeByte(ElementState.NEW.getByteValue());
                writeProperties(getProperties(edge), schema, out);
                out.writeBoolean(false);
                WritableUtils.writeVLong(out, 0);
                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge.getVertex(direction.opposite())));
            }
        }
    }

}
