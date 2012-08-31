package com.thinkaurelius.faunus.util;

import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.mapreduce.util.CounterMap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexToFaunusBinary {

    public static void write(final Vertex vertex, final DataOutput out) throws IOException {
        writeId(vertex.getId(), out);
        out.writeInt(0);
        writeEdges(vertex, Direction.IN, out);
        writeEdges(vertex, Direction.OUT, out);
        writeProperties(vertex, out);
    }

    private static void writeEdges(final Vertex vertex, final Direction direction, final DataOutput out) throws IOException {
        final CounterMap<String> map = new CounterMap<String>();
        for (final Edge edge : vertex.getEdges(direction)) {
            map.incr(edge.getLabel(), 1);
        }
        out.writeShort(map.size());
        for (final Map.Entry<String, Long> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().intValue());
            for (final Edge edge : vertex.getEdges(direction, entry.getKey())) {
                writeId(edge.getId(), out);
                out.writeInt(0);
                writeId(edge.getVertex(direction.opposite()).getId(), out);
                writeProperties(edge, out);
            }
        }
    }

    private static void writeId(final Object id, final DataOutput out) throws IOException {
        if (id instanceof Long)
            out.writeLong((Long) id);
        else if (id instanceof Number)
            out.writeLong(((Number) id).longValue());
        else
            out.writeLong(Long.valueOf(id.toString()));
    }

    private static void writeProperties(final Element element, final DataOutput out) throws IOException {
        out.writeShort(element.getPropertyKeys().size());
        for (final String key : element.getPropertyKeys()) {
            out.writeUTF(key);
            final Object valueObject = element.getProperty(key);
            final Class valueClass = valueObject.getClass();

            if (valueClass.equals(Integer.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.INT.val);
                out.writeInt((Integer) valueObject);
            } else if (valueClass.equals(Long.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.LONG.val);
                out.writeLong((Long) valueObject);
            } else if (valueClass.equals(Float.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.FLOAT.val);
                out.writeFloat((Float) valueObject);
            } else if (valueClass.equals(Double.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.DOUBLE.val);
                out.writeDouble((Double) valueObject);
            } else if (valueClass.equals(String.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.STRING.val);
                out.writeUTF((String) valueObject);
            } else if (valueClass.equals(Boolean.class)) {
                out.writeByte(FaunusElement.ElementProperties.PropertyType.BOOLEAN.val);
                out.writeBoolean((Boolean) valueObject);
            } else {
                throw new IOException("Property value type of " + valueClass + " is not supported");
            }
        }
    }
}
