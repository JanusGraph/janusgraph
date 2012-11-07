package com.thinkaurelius.faunus.formats.rexster.util;

import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.mapreduce.util.CounterMap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

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
        WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(vertex));
        out.writeBoolean(false);
        WritableUtils.writeVInt(out, 0);
        writeProperties(vertex, out);

        writeEdges(vertex, Direction.IN, out);
        writeEdges(vertex, Direction.OUT, out);

    }

    private void writeEdges(final Vertex vertex, final Direction direction, final DataOutput out) throws IOException {
        final CounterMap<String> map = new CounterMap<String>();
        for (final Edge edge : vertex.getEdges(direction)) {
            map.incr(edge.getLabel(), 1);
        }
        out.writeShort(map.size());
        for (final Map.Entry<String, Long> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            WritableUtils.writeVInt(out, entry.getValue().intValue());
            for (final Edge edge : vertex.getEdges(direction, entry.getKey())) {
                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge));
                out.writeBoolean(false);
                WritableUtils.writeVInt(out, 0);
                writeProperties(edge, out);
                WritableUtils.writeVLong(out, elementIdHandler.convertIdentifier(edge.getVertex(direction.opposite())));
            }
        }
    }

    private static void writeProperties(final Element element, final DataOutput out) throws IOException {
        out.writeShort(element.getPropertyKeys().size());
        for (final String key : element.getPropertyKeys()) {
            out.writeUTF(key);
            final Object valueObject = element.getProperty(key);
            final Class valueClass = valueObject.getClass();

            if (valueClass.equals(Integer.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.INT.val);
                WritableUtils.writeVInt(out, (Integer) valueObject);
            } else if (valueClass.equals(Long.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.LONG.val);
                WritableUtils.writeVLong(out, (Long) valueObject);
            } else if (valueClass.equals(Float.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.FLOAT.val);
                out.writeFloat((Float) valueObject);
            } else if (valueClass.equals(Double.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.DOUBLE.val);
                out.writeDouble((Double) valueObject);
            } else if (valueClass.equals(String.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.STRING.val);
                WritableUtils.writeString(out, (String) valueObject);
            } else if (valueClass.equals(Boolean.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.BOOLEAN.val);
                out.writeBoolean((Boolean) valueObject);
            } else {
                throw new IOException("Property value type of " + valueClass + " is not supported");
            }
        }
    }
}
