package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.util.ReadArrayBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import com.thinkaurelius.titan.hadoop.FaunusPathElement.MicroElement;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.KRYO_MAX_OUTPUT_SIZE;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusSerializer {

    // This is volatile to support double-checked locking
    private static volatile Serializer standardSerializer;

    private final FaunusTypeManager types;
    private final boolean trackState;
    private final boolean trackPaths;
    private final Configuration configuration;

    private static final Logger log =
            LoggerFactory.getLogger(FaunusSerializer.class);

    public FaunusSerializer(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        this.types = FaunusTypeManager.getTypeManager(configuration);
        this.configuration = configuration;
        this.trackState = configuration.get(TitanHadoopConfiguration.PIPELINE_TRACK_STATE);
        this.trackPaths = configuration.get(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS);
    }

    public void writeVertex(final FaunusVertex vertex, final DataOutput out) throws IOException {
        //Need to write the id up front for the comparator
        WritableUtils.writeVLong(out, vertex.id);
        Schema schema = new Schema();
        vertex.updateSchema(schema);
        schema.writeSchema(out);
        writePathElement(vertex, schema, out);
        writeEdges(vertex, vertex.inAdjacency, out, Direction.IN, schema);
        FaunusVertexLabel vl = (FaunusVertexLabel)vertex.getVertexLabel();
        out.writeUTF(vl.isDefault()?"":vl.getName());
    }

    public void readVertex(final FaunusVertex vertex, final DataInput in) throws IOException {
        WritableUtils.readVLong(in);
        Schema schema = readSchema(in);
        readPathElement(vertex, schema, in);
        vertex.inAdjacency = readEdges(vertex, in, Direction.IN, schema);
        String labelName = in.readUTF();
        vertex.setVertexLabel(StringUtils.isBlank(labelName)?FaunusVertexLabel.DEFAULT_VERTEXLABEL:
                                    types.getVertexLabel(labelName));
    }

    public void writeEdge(final StandardFaunusEdge edge, final DataOutput out) throws IOException {
        writePathElement(edge, out);
        WritableUtils.writeVLong(out, edge.inVertex);
        WritableUtils.writeVLong(out, edge.outVertex);
        writeFaunusType(edge.getType(), out);
    }

    public void readEdge(final StandardFaunusEdge edge, final DataInput in) throws IOException {
        readPathElement(edge, in);
        edge.inVertex = WritableUtils.readVLong(in);
        edge.outVertex = WritableUtils.readVLong(in);
        edge.setLabel((FaunusEdgeLabel)readFaunusType(in));
    }

    public void writeProperty(final StandardFaunusProperty property, final DataOutput out) throws IOException {
        writePathElement(property, out);
        WritableUtils.writeVLong(out, property.vertexid);
        serializeObject(out,property.getValue());
        writeFaunusType(property.getType(), out);
    }

    public void readProperty(final StandardFaunusProperty property, final DataInput in) throws IOException {
        readPathElement(property, in);
        property.vertexid = WritableUtils.readVLong(in);
        property.value = deserializeObject(in);
        property.setKey((FaunusPropertyKey)readFaunusType(in));
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
            List<List<MicroElement>> paths = readElementPaths(in);
            element.tracker = new FaunusPathElement.Tracker(paths,
                    (element instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(element.id) : new StandardFaunusEdge.MicroEdge(element.id));

            log.trace("readPathElement element={} paths={}", element, paths);
        } else {
            element.pathCounter = WritableUtils.readVLong(in);
            element.tracker = FaunusPathElement.DEFAULT_TRACK;
        }
    }

    private void writePathElement(final FaunusPathElement element, final Schema schema, final DataOutput out) throws IOException {
        writeElement(element, schema, out);
        if (trackPaths)
            writeElementPaths(element.tracker.paths, out);
        else
            WritableUtils.writeVLong(out, element.pathCounter);
    }

    private void readElement(final FaunusElement element, Schema schema, final DataInput in) throws IOException {
        element.id = WritableUtils.readVLong(in);
        if (trackState) element.setLifeCycle(in.readByte());
        element.outAdjacency = readEdges(element,in,Direction.OUT,schema);
    }

    private void writeElement(final FaunusElement element, final Schema schema, final DataOutput out) throws IOException {
        Preconditions.checkArgument(trackState || !element.isRemoved());
        WritableUtils.writeVLong(out, element.id);
        if (trackState) out.writeByte(element.getLifeCycle());
        writeEdges(element, element.outAdjacency, out, Direction.OUT, schema);
    }



    private void serializeObject(final DataOutput out, Object value) throws IOException {
        final com.thinkaurelius.titan.graphdb.database.serialize.DataOutput o = getStandardSerializer().getDataOutput(40);
        o.writeClassAndObject(value);
        final StaticBuffer buffer = o.getStaticBuffer();
        WritableUtils.writeVInt(out, buffer.length());
        out.write(buffer.as(StaticBuffer.ARRAY_FACTORY));
    }

    private Object deserializeObject(final DataInput in) throws IOException {
        int byteLength = WritableUtils.readVInt(in);
        byte[] bytes = new byte[byteLength];
        in.readFully(bytes);
        final ReadBuffer buffer = new ReadArrayBuffer(bytes);
        return getStandardSerializer().readClassAndObject(buffer);
    }

    /**
     * Return the StandardSerializer singleton shared between all instances of FaunusSerializer.
     *
     * If it has not yet been initialized, then the singleton is created using the maximum
     * Kryo buffer size configured in the calling FaunusSerializer.
     *
     * @return
     */
    private Serializer getStandardSerializer() {
        if (null == standardSerializer) { // N.B. standardSerializer is volatile
            synchronized (FaunusSerializer.class) {
                if (null == standardSerializer) {
                    int maxOutputBufSize = configuration.get(KRYO_MAX_OUTPUT_SIZE);
                    standardSerializer = new StandardSerializer(true, maxOutputBufSize);
                }
            }
        }
        // TODO consider checking whether actual output buffer size matches config, create new StandardSerializer if mismatched?  Might not be worth it
        return standardSerializer;
    }

    private <T extends FaunusRelation> Iterable<T> filterDeletedRelations(Iterable<T> elements) {
        if (trackState) return elements;
        else return Iterables.filter(elements, new Predicate<T>() {
            @Override
            public boolean apply(@Nullable T element) {
                return !element.isRemoved();
            }
        });
    }

    private SetMultimap<FaunusRelationType, FaunusRelation> readEdges(final FaunusElement element, final DataInput in, final Direction direction, final Schema schema) throws IOException {
        final SetMultimap<FaunusRelationType, FaunusRelation> adjacency = HashMultimap.create();
        int numTypes = WritableUtils.readVInt(in);
        for (int i = 0; i < numTypes; i++) {
            FaunusRelationType type;
            if (schema == null) type = readFaunusType(in);
            else type = schema.getType(WritableUtils.readVLong(in));
            final int size = WritableUtils.readVInt(in);
            for (int j = 0; j < size; j++) {
                FaunusRelation relation;
                if (element instanceof FaunusVertex) {
                    if (type.isEdgeLabel()) {
                        final StandardFaunusEdge edge = new StandardFaunusEdge(configuration);
                        edge.setLabel((FaunusEdgeLabel)type);
                        readPathElement(edge, schema, in);
                        long otherId = WritableUtils.readVLong(in);
                        switch (direction) {
                            case IN:
                                edge.inVertex = element.getLongId();
                                edge.outVertex = otherId;
                                break;
                            case OUT:
                                edge.outVertex = element.getLongId();
                                edge.inVertex = otherId;
                                break;
                            default:
                                throw ExceptionFactory.bothIsNotSupported();
                        }
                        relation = edge;
                        log.trace("readEdges edge={} paths={}", edge, edge.tracker.paths);
                    } else {
                        assert type.isPropertyKey() && direction==Direction.OUT;
                        final StandardFaunusProperty property = new StandardFaunusProperty(configuration);
                        property.setKey((FaunusPropertyKey) type);
                        readPathElement(property, schema, in);
                        property.value = deserializeObject(in);
                        relation = property;
                    }
                } else {
                    byte lifecycle = trackState?in.readByte():-1;
                    if (type.isEdgeLabel()) {
                        relation = new SimpleFaunusEdge((FaunusEdgeLabel)type,new FaunusVertex(configuration,WritableUtils.readVLong(in)));
                    } else {
                        assert type.isPropertyKey() && direction==Direction.OUT;
                        relation = new SimpleFaunusProperty((FaunusPropertyKey)type,deserializeObject(in));
                    }
                    if (trackState) relation.setLifeCycle(lifecycle);
                }
                adjacency.put(type, relation);
            }
        }
        if (adjacency.isEmpty()) return FaunusElement.EMPTY_ADJACENCY;
        return adjacency;
    }

    private void writeEdges(final FaunusElement element, final SetMultimap<FaunusRelationType, FaunusRelation> edges, final DataOutput out, final Direction direction, final Schema schema) throws IOException {
        Map<FaunusRelationType, Integer> counts = Maps.newHashMap();
        int typeCount = 0;
        for (FaunusRelationType type : edges.keySet()) {
            int count = IterablesUtil.size(filterDeletedRelations(edges.get(type)));
            counts.put(type, count);
            if (count > 0) typeCount++;
        }

        WritableUtils.writeVInt(out, typeCount);
        for (FaunusRelationType type : edges.keySet()) {
            if (counts.get(type) == 0) continue;

            if (schema == null) writeFaunusType(type, out);
            else WritableUtils.writeVLong(out, schema.getTypeId(type));

            WritableUtils.writeVInt(out, counts.get(type));
            Iterable<FaunusRelation> subset = filterDeletedRelations(edges.get(type));
            for (final FaunusRelation rel : subset) {
                if (element instanceof FaunusVertex) {
                    assert rel instanceof StandardFaunusRelation;
                    writePathElement((StandardFaunusRelation)rel,schema,out);
                } else {
                    assert rel instanceof SimpleFaunusRelation;
                    if (trackState) out.writeByte(((SimpleFaunusRelation)rel).getLifeCycle());
                }
                if (rel.isEdge()) {
                    WritableUtils.writeVLong(out, ((FaunusEdge)rel).getVertexId(direction.opposite()));
                } else {
                    serializeObject(out,((FaunusProperty)rel).getValue());
                }
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
                        path.add(new StandardFaunusEdge.MicroEdge(WritableUtils.readVLong(in)));
                }
                paths.add(path);
            }
            return paths;
        }
    }

    private void writeFaunusType(final FaunusRelationType type, final DataOutput out) throws IOException {
        out.writeByte(type.isPropertyKey()?0:1);
        out.writeUTF(type.getName());
    }

    private FaunusRelationType readFaunusType(final DataInput in) throws IOException {
        int type = in.readByte();
        String typeName = in.readUTF();
        assert type==0 || type==1;
        if (type==0) return types.getPropertyKey(typeName);
        else return types.getEdgeLabel(typeName);
    }


    class Schema {

        private final BiMap<FaunusRelationType, Long> localTypes;
        private long count = 1;

        private Schema() {
            this(8);
        }

        private Schema(int size) {
            this.localTypes = HashBiMap.create(size);
        }

        void add(String type) {
            this.add(types.getRelationType(type));
        }

        void add(FaunusRelationType type) {
            if (!localTypes.containsKey(type)) localTypes.put(type, count++);
        }

        void addAll(Iterable<FaunusRelationType> types) {
            for (FaunusRelationType type : types) add(type);
        }

        long getTypeId(FaunusRelationType type) {
            Long id = localTypes.get(type);
            Preconditions.checkArgument(id != null, "Type is not part of the schema: " + type);
            return id;
        }

        FaunusRelationType getType(long id) {
            FaunusRelationType type = localTypes.inverse().get(id);
            Preconditions.checkArgument(type != null, "Type is not part of the schema: " + id);
            return type;
        }

        private void add(FaunusRelationType type, long index) {
            Preconditions.checkArgument(!localTypes.containsValue(index));
            localTypes.put(type, index);
            count = index + 1;
        }

        private void writeSchema(final DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, localTypes.size());
            for (Map.Entry<FaunusRelationType, Long> entry : localTypes.entrySet()) {
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
                return ((Long) (((FaunusElement) a).getLongId())).compareTo(((FaunusElement) b).getLongId());
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
//    public static interface ElementIdHandler {
//        long convertIdentifier(final Element element);
//    }
//
//    public void writeVertex(final Vertex vertex, final ElementIdHandler elementIdHandler, final DataOutput out) throws IOException {
//        Schema schema = new Schema();
//        //Convert properties and update schema
//        Multimap<HadoopType, FaunusProperty> properties = getProperties(vertex);
//        for (HadoopType type : properties.keySet()) schema.add(type);
//        for (Edge edge : vertex.getEdges(Direction.BOTH)) {
//            schema.add(edge.getLabel());
//            for (String key : edge.getPropertyKeys()) schema.add(key);
//        }
//
//        WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(vertex));
//        schema.writeSchema(out);
//        WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(vertex));
//        if (trackState) out.writeByte(ElementState.NEW.getByteValue());
//        writeProperties(properties, schema, out);
//        out.writeBoolean(false);
//        WritableUtils.writeVLong(out, 0);
//        writeEdges(vertex, Direction.IN, elementIdHandler, schema, out);
//        writeEdges(vertex, Direction.OUT, elementIdHandler, schema, out);
//
//    }
//
//    private Multimap<HadoopType, FaunusProperty> getProperties(Element element) {
//        Multimap<HadoopType, FaunusProperty> properties = HashMultimap.create();
//        for (String key : element.getPropertyKeys()) {
//            HadoopType type = types.get(key);
//            properties.put(type, new FaunusProperty(type, element.getProperty(key)));
//        }
//        return properties;
//    }
//
//    private void writeEdges(final Vertex vertex, final Direction direction, final ElementIdHandler elementIdHandler,
//                            final Schema schema, final DataOutput out) throws IOException {
//        final Multiset<String> labelCount = HashMultiset.create();
//        for (final Edge edge : vertex.getEdges(direction)) {
//            labelCount.add(edge.getLabel());
//        }
//        WritableUtils.writeVInt(out, labelCount.elementSet().size());
//        for (String label : labelCount.elementSet()) {
//            HadoopType type = types.get(label);
//            WritableUtils.writeVLong(out, schema.getTypeId(type));
//            WritableUtils.writeVInt(out, labelCount.count(label));
//            for (final Edge edge : vertex.getEdges(direction, label)) {
//                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge));
//                if (trackState) out.writeByte(ElementState.NEW.getByteValue());
//                writeProperties(getProperties(edge), schema, out);
//                out.writeBoolean(false);
//                WritableUtils.writeVLong(out, 0);
//                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge.getVertex(direction.opposite())));
//            }
//        }
//    }

}
