package com.thinkaurelius.titan.hadoop;

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
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ReadArrayBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import com.thinkaurelius.titan.hadoop.HadoopPathElement.MicroElement;
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
public class HadoopSerializer {

    private final Serializer serializer;
    private final HadoopType.Manager types;
    private final boolean trackState;
    private final boolean trackPaths;
    private final Configuration configuration;

    public HadoopSerializer(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        // TODO titan05 integration -- formerly new KryoSerializer(true); is StandardSerializer substitution OK?
        this.serializer = new StandardSerializer(true);
        this.types = HadoopType.DEFAULT_MANAGER;
        this.configuration = configuration;
        this.trackState = configuration.getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_STATE, false);
        this.trackPaths = configuration.getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, false);
    }

    public void writeVertex(final HadoopVertex vertex, final DataOutput out) throws IOException {
        //Need to write the id up front for the comparator
        WritableUtils.writeVLong(out, vertex.id);
        Schema schema = new Schema();
        vertex.updateSchema(schema);
        schema.writeSchema(out);
        writePathElement(vertex, schema, out);
        writeEdges(vertex.inEdges, out, Direction.OUT, schema);
        writeEdges(vertex.outEdges, out, Direction.IN, schema);
    }

    public void readVertex(final HadoopVertex vertex, final DataInput in) throws IOException {
        WritableUtils.readVLong(in);
        Schema schema = readSchema(in);
        readPathElement(vertex, schema, in);
        vertex.inEdges = readEdges(in, Direction.OUT, vertex.id, schema);
        vertex.outEdges = readEdges(in, Direction.IN, vertex.id, schema);
    }

    public void writeEdge(final HadoopEdge edge, final DataOutput out) throws IOException {
        writePathElement(edge, out);
        WritableUtils.writeVLong(out, edge.inVertex);
        WritableUtils.writeVLong(out, edge.outVertex);
        writeHadoopType(edge.getType(), out);
    }

    public void readEdge(final HadoopEdge edge, final DataInput in) throws IOException {
        readPathElement(edge, in);
        edge.inVertex = WritableUtils.readVLong(in);
        edge.outVertex = WritableUtils.readVLong(in);
        edge.setLabel(readHadoopType(in));
    }

    private void readPathElement(final HadoopPathElement element, final DataInput in) throws IOException {
        readPathElement(element, null, in);
    }

    private void writePathElement(final HadoopPathElement element, final DataOutput out) throws IOException {
        writePathElement(element, null, out);
    }

    private void readPathElement(final HadoopPathElement element, Schema schema, final DataInput in) throws IOException {
        readElement(element, schema, in);
        if (trackPaths) {
            element.paths = readElementPaths(in);
            element.microVersion = (element instanceof HadoopVertex) ? new HadoopVertex.MicroVertex(element.id) : new HadoopEdge.MicroEdge(element.id);
            element.trackPaths = true;
        } else {
            element.pathCounter = WritableUtils.readVLong(in);
            element.trackPaths = false;
        }
    }

    private void writePathElement(final HadoopPathElement element, final Schema schema, final DataOutput out) throws IOException {
        writeElement(element, schema, out);
        if (trackPaths)
            writeElementPaths(element.paths, out);
        else
            WritableUtils.writeVLong(out, element.pathCounter);
    }

    private void readElement(final HadoopElement element, Schema schema, final DataInput in) throws IOException {
        element.id = WritableUtils.readVLong(in);
        if (trackState) element.setState(ElementState.valueOf(in.readByte()));
        element.properties = readProperties(schema, in);
    }

    private void writeElement(final HadoopElement element, final Schema schema, final DataOutput out) throws IOException {
        Preconditions.checkArgument(trackState || !element.isDeleted());
        WritableUtils.writeVLong(out, element.id);
        if (trackState) out.writeByte(element.getState().getByteValue());
        writeProperties(element.properties, schema, out);
    }

    private <T extends HadoopElement> Iterable<T> filterDeleted(Iterable<T> elements) {
        if (trackState) return elements;
        else return Iterables.filter(elements, new Predicate<T>() {
            @Override
            public boolean apply(@Nullable T element) {
                return !element.isDeleted();
            }
        });
    }

    private void writeProperties(final Multimap<HadoopType, HadoopProperty> properties, final Schema schema, final DataOutput out) throws IOException {
        Iterable<HadoopProperty> subset = filterDeleted(properties.values());
        int count = IterablesUtil.size(subset);
        WritableUtils.writeVInt(out, count);
        if (count > 0) {
            for (final HadoopProperty property : subset) {
                //Type
                if (schema == null) writeHadoopType(property.getType(), out);
                else WritableUtils.writeVLong(out, schema.getTypeId(property.getType()));
                //Value
                // TODO titan05 integration -- this used to be getDataOutput(40, true), what happened to the true argument?
                final com.thinkaurelius.titan.graphdb.database.serialize.DataOutput o = serializer.getDataOutput(40);
                o.writeClassAndObject(property.getValue());
                final StaticBuffer buffer = o.getStaticBuffer();
                WritableUtils.writeVInt(out, buffer.length());
                out.write(buffer.as(StaticBuffer.ARRAY_FACTORY));
                //Element
                writeElement(property, schema, out);
            }
        }
    }

    private Multimap<HadoopType, HadoopProperty> readProperties(final Schema schema, final DataInput in) throws IOException {
        final int count = WritableUtils.readVInt(in);
        if (count == 0)
            return HadoopElement.NO_PROPERTIES;
        else {
            final Multimap<HadoopType, HadoopProperty> properties = HashMultimap.create();
            for (int i = 0; i < count; i++) {
                HadoopType type;
                if (schema == null) type = readHadoopType(in);
                else type = schema.getType(WritableUtils.readVLong(in));
                //Value
                int byteLength = WritableUtils.readVInt(in);
                byte[] bytes = new byte[byteLength];
                in.readFully(bytes);
                final ReadBuffer buffer = new ReadArrayBuffer(bytes);
                Object value = serializer.readClassAndObject(buffer);

                HadoopProperty property = new HadoopProperty(type, value);
                readElement(property, schema, in);
                properties.put(type, property);
            }
            return properties;
        }
    }

    private ListMultimap<HadoopType, HadoopEdge> readEdges(final DataInput in, final Direction idToRead, final long otherId, final Schema schema) throws IOException {
        final ListMultimap<HadoopType, HadoopEdge> edges = ArrayListMultimap.create();
        int edgeTypes = WritableUtils.readVInt(in);
        for (int i = 0; i < edgeTypes; i++) {
            HadoopType type = schema.getType(WritableUtils.readVLong(in));
            final int size = WritableUtils.readVInt(in);
            for (int j = 0; j < size; j++) {
                final HadoopEdge edge = new HadoopEdge(this.configuration);
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

    private void writeEdges(final ListMultimap<HadoopType, HadoopEdge> edges, final DataOutput out, final Direction idToWrite, final Schema schema) throws IOException {
        Map<HadoopType, Integer> counts = Maps.newHashMap();
        int typeCount = 0;
        for (HadoopType type : edges.keySet()) {
            int count = IterablesUtil.size(filterDeleted(edges.get(type)));
            counts.put(type, count);
            if (count > 0) typeCount++;
        }

        WritableUtils.writeVInt(out, typeCount);
        for (HadoopType type : edges.keySet()) {
            if (counts.get(type) == 0) continue;
            Iterable<HadoopEdge> subset = filterDeleted(edges.get(type));
            WritableUtils.writeVLong(out, schema.getTypeId(type));
            WritableUtils.writeVInt(out, counts.get(type));
            for (final HadoopEdge edge : subset) {
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
                    if (element instanceof HadoopVertex.MicroVertex)
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
                        path.add(new HadoopVertex.MicroVertex(WritableUtils.readVLong(in)));
                    else
                        path.add(new HadoopEdge.MicroEdge(WritableUtils.readVLong(in)));
                }
                paths.add(path);
            }
            return paths;
        }
    }

    private void writeHadoopType(final HadoopType type, final DataOutput out) throws IOException {
        out.writeUTF(type.getName());
    }

    private HadoopType readHadoopType(final DataInput in) throws IOException {
        return types.get(in.readUTF());
    }


    class Schema {

        private final BiMap<HadoopType, Long> localTypes;
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

        void add(HadoopType type) {
            if (!localTypes.containsKey(type)) localTypes.put(type, count++);
        }

        void addAll(Iterable<HadoopType> types) {
            for (HadoopType type : types) add(type);
        }

        long getTypeId(HadoopType type) {
            Long id = localTypes.get(type);
            Preconditions.checkArgument(id != null, "Type is not part of the schema: " + type);
            return id;
        }

        HadoopType getType(long id) {
            HadoopType type = localTypes.inverse().get(id);
            Preconditions.checkArgument(type != null, "Type is not part of the schema: " + id);
            return type;
        }

        private void add(HadoopType type, long index) {
            Preconditions.checkArgument(!localTypes.containsValue(index));
            localTypes.put(type, index);
            count = index + 1;
        }

        private void writeSchema(final DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, localTypes.size());
            for (Map.Entry<HadoopType, Long> entry : localTypes.entrySet()) {
                writeHadoopType(entry.getKey(), out);
                WritableUtils.writeVLong(out, entry.getValue());
            }
        }

    }

    private Schema readSchema(final DataInput in) throws IOException {
        int size = WritableUtils.readVInt(in);
        Schema schema = new Schema(size);
        for (int i = 0; i < size; i++) {
            schema.add(readHadoopType(in), WritableUtils.readVLong(in));
        }
        return schema;
    }

    static {
        WritableComparator.define(HadoopPathElement.class, new Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(HadoopPathElement.class);
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
            if (a instanceof HadoopElement && b instanceof HadoopElement)
                return ((Long) (((HadoopElement) a).getIdAsLong())).compareTo(((HadoopElement) b).getIdAsLong());
            else
                return super.compare(a, b);
        }
    }

    //################################################
    // Serialization for vanilla Blueprints
    //################################################


    /**
     * All graph element identifiers must be of the long data type.  Implementations of this
     * interface makes it possible to control the conversion of the identifier in the
     * VertexToHadoopBinary utility class.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    public static interface ElementIdHandler {
        long convertIdentifier(final Element element);
    }

    public void writeVertex(final Vertex vertex, final ElementIdHandler elementIdHandler, final DataOutput out) throws IOException {
        Schema schema = new Schema();
        //Convert properties and update schema
        Multimap<HadoopType, HadoopProperty> properties = getProperties(vertex);
        for (HadoopType type : properties.keySet()) schema.add(type);
        for (Edge edge : vertex.getEdges(Direction.BOTH)) {
            schema.add(edge.getLabel());
            for (String key : edge.getPropertyKeys()) schema.add(key);
        }

        WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(vertex));
        schema.writeSchema(out);
        WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(vertex));
        if (trackState) out.writeByte(ElementState.NEW.getByteValue());
        writeProperties(properties, schema, out);
        out.writeBoolean(false);
        WritableUtils.writeVLong(out, 0);
        writeEdges(vertex, Direction.IN, elementIdHandler, schema, out);
        writeEdges(vertex, Direction.OUT, elementIdHandler, schema, out);

    }

    private Multimap<HadoopType, HadoopProperty> getProperties(Element element) {
        Multimap<HadoopType, HadoopProperty> properties = HashMultimap.create();
        for (String key : element.getPropertyKeys()) {
            HadoopType type = types.get(key);
            properties.put(type, new HadoopProperty(type, element.getProperty(key)));
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
            HadoopType type = types.get(label);
            WritableUtils.writeVLong(out, schema.getTypeId(type));
            WritableUtils.writeVInt(out, labelCount.count(label));
            for (final Edge edge : vertex.getEdges(direction, label)) {
                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge));
                if (trackState) out.writeByte(ElementState.NEW.getByteValue());
                writeProperties(getProperties(edge), schema, out);
                out.writeBoolean(false);
                WritableUtils.writeVLong(out, 0);
                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge.getVertex(direction.opposite())));
            }
        }
    }

}
