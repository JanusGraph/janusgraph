package com.thinkaurelius.faunus.io.graph.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ElementProperties implements Writable {

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

    protected Map<String, Object> properties;

    public ElementProperties(final Map<String, Object> properties) {
        this.properties = properties;
    }

    public ElementProperties() {
        this.properties = new HashMap<String, Object>();
    }

    public void write(final DataOutput out) throws IOException {
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
}

