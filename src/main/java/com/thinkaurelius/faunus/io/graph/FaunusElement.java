package com.thinkaurelius.faunus.io.graph;

import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement<T extends Element> implements Element, WritableComparable<T> {

    protected Map<String, Object> properties = new HashMap<String, Object>();
    protected long id;

    public enum ElementType {
        VERTEX((byte) 0),
        EDGE((byte) 1);
        public byte val;

        private ElementType(byte v) {
            this.val = v;
        }
    }

    public enum PropertyType {
        INT((byte) 0),
        LONG((byte) 1),
        FLOAT((byte) 2),
        DOUBLE((byte) 3),
        STRING((byte) 0);
        public byte val;

        private PropertyType(byte v) {
            this.val = v;
        }
    }


    public FaunusElement(final Long id) {
        this.id = id;
    }

    public void setProperty(String key, Object value) {
        this.properties.put(key, value);
    }

    public Object removeProperty(String key) {
        return this.properties.remove(key);
    }

    public Object getProperty(String key) {
        return this.properties.get(key);
    }

    public Set<String> getPropertyKeys() {
        return this.properties.keySet();
    }

    public Object getId() {
        return id;
    }

    public void write(final DataOutput out) throws IOException {
        if (this instanceof Vertex)
            out.writeByte(ElementType.VERTEX.val);
        else
            out.writeByte(ElementType.EDGE.val);

        out.writeLong(this.id);

        out.writeInt(this.properties.size());
        for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
            out.writeUTF(entry.getKey());
            final Class valueClass = entry.getValue().getClass();
            final Object valueObject = entry.getValue();
            if (valueClass.equals(Integer.class)) {
                out.writeByte(PropertyType.INT.val);
                out.writeInt((Integer) valueObject);
            } else if (valueClass.equals(Long.class)) {
                out.writeByte(PropertyType.LONG.val);
                out.writeLong((Long) valueObject);
            } else if (valueClass.equals(Float.class)) {
                out.writeByte(PropertyType.FLOAT.val);
                out.writeFloat((Float) valueObject);
            } else if (valueClass.equals(Double.class)) {
                out.writeByte(PropertyType.DOUBLE.val);
                out.writeDouble((Double) valueObject);
            } else if (valueClass.equals(String.class)) {
                out.writeByte(PropertyType.STRING.val);
                out.writeUTF((String) valueObject);
            } else {
                throw new IOException("Property value type of " + valueClass + " is not supported");
            }
        }
    }

    public void readFields(final DataInput in) throws IOException {
        in.readByte();
        this.id = in.readLong();

        final int numberOfProperties = in.readInt();
        for (int i = 0; i < numberOfProperties; i++) {
            final String key = in.readUTF();
            final byte valueClass = in.readByte();
            final Object valueObject;
            if (valueClass == PropertyType.INT.val) {
                valueObject = in.readInt();
            } else if (valueClass == PropertyType.LONG.val) {
                valueObject = in.readLong();
            } else if (valueClass == PropertyType.FLOAT.val) {
                valueObject = in.readFloat();
            } else if (valueClass == PropertyType.DOUBLE.val) {
                valueObject = in.readDouble();
            } else if (valueClass == PropertyType.STRING.val) {
                valueObject = in.readUTF();
            } else {
                throw new IOException("Property value type of " + valueClass + " is not supported");
            }
            this.properties.put(key, valueObject);
        }
    }

    public int compareTo(final T other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }
}
