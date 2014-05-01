package com.thinkaurelius.titan.hadoop.formats.rexster.util;

import com.thinkaurelius.titan.hadoop.FaunusSerializer;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Vertex;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexToFaunusBinary {

    private static final ElementIdHandler DEFAULT_ELEMENT_ID_HANDLER = new DefaultElementIdHandler();
    private final ElementIdHandler elementIdHandler;

    public VertexToFaunusBinary() {
        this(DEFAULT_ELEMENT_ID_HANDLER);
    }

    public VertexToFaunusBinary(final ElementIdHandler elementIdHandler) {
        this.elementIdHandler = elementIdHandler;
    }

    public static void write(final Vertex vertex, final DataOutput out) throws IOException {
        new VertexToFaunusBinary().writeVertex(vertex, out);
    }

    public static void write(final Vertex vertex, final DataOutput out,
                             final ElementIdHandler elementIdHandler) throws IOException {
        new VertexToFaunusBinary(elementIdHandler).writeVertex(vertex, out);
    }

    public void writeVertex(final Vertex vertex, final DataOutput out) throws IOException {
        new FaunusSerializer(new EmptyConfiguration()).writeVertex(vertex, elementIdHandler, out);
    }

//        WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(vertex));
//        out.writeBoolean(false);
//        WritableUtils.writeVLong(out, 0);
//        writeProperties(vertex, out);
//        writeEdges(vertex, Direction.IN, out);
//        writeEdges(vertex, Direction.OUT, out);
//
//    }
//
//    private void writeEdges(final Vertex vertex, final Direction direction, final DataOutput out) throws IOException {
//        final CounterMap<String> map = new CounterMap<String>();
//        for (final Edge edge : vertex.getEdges(direction)) {
//            map.incr(edge.getLabel(), 1);
//        }
//        WritableUtils.writeVInt(out, map.size());
//        for (final Map.Entry<String, Long> entry : map.entrySet()) {
//            out.writeUTF(entry.getKey());
//            WritableUtils.writeVInt(out, entry.getValue().intValue());
//            for (final Edge edge : vertex.getEdges(direction, entry.getKey())) {
//                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge));
//                out.writeBoolean(false);
//                WritableUtils.writeVLong(out, 0);
//                writeProperties(edge, out);
//                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge.getVertex(direction.opposite())));
//            }
//        }
//    }
//
//    private static void writeProperties(final Element element, final DataOutput out) throws IOException {
//        WritableUtils.writeVInt(out, element.getPropertyKeys().size());
//        if (element.getPropertyKeys().size() > 0) {
//            final com.thinkaurelius.titan.graphdb.database.serialize.DataOutput o = serialize.getDataOutput(128, true);
//            for (final String key : element.getPropertyKeys()) {
//                o.writeObject(key, String.class);
//                o.writeClassAndObject(element.getProperty(key));
//            }
//            final StaticBuffer buffer = o.getStaticBuffer();
//            WritableUtils.writeVInt(out, buffer.length());
//            out.write(ByteBufferUtil.getArray(buffer.asByteBuffer()));
//        }
//    }
}
