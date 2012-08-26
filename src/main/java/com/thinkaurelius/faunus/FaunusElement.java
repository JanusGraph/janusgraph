package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.util.MicroEdge;
import com.thinkaurelius.faunus.util.MicroElement;
import com.thinkaurelius.faunus.util.MicroVertex;
import com.tinkerpop.blueprints.Element;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement implements Element, Writable {


    protected static final Map<String, String> TYPE_MAP = new HashMap<String, String>() {
        @Override
        public final String get(final Object object) {
            final String label = (String) object;
            final String existing = super.get(label);
            if (null == existing) {
                super.put(label, label);
                return label;
            } else {
                return existing;
            }
        }
    };

    public static final Set<Class<?>> SUPPORTED_ATTRIBUTE_TYPES = new HashSet<Class<?>>() {{
        add(Integer.class);
        add(Long.class);
        add(Float.class);
        add(Double.class);
        add(String.class);
    }};


    protected long id;
    protected Map<String, Object> properties = null;
    protected List<List<MicroElement>> paths = new ArrayList<List<MicroElement>>();

    public FaunusElement(final long id) {
        this.id = id;
    }

    public void addPath(final List<MicroElement> path) {
        this.paths.add(path);
    }

    public List<List<MicroElement>> getPaths() {
        return this.paths;
    }

    public boolean hasPaths() {
        return !this.paths.isEmpty();
    }

    public void clearPaths() {
        this.paths.clear();
    }

    public int pathCount() {
        return this.paths.size();
    }

    public void incrPath() {
        if (paths.size() == 0) {
            this.paths.add(new ArrayList<MicroElement>());
        }
        final List<MicroElement> path = this.paths.get(paths.size() - 1);
        if (this instanceof FaunusVertex) {
            path.add(new MicroVertex(this.id));
        } else {
            path.add(new MicroEdge(this.id));
        }
    }

    public void setProperty(final String key, final Object value) {
        if (null == this.properties)
            this.properties = new HashMap<String, Object>();
        this.properties.put(TYPE_MAP.get(key), value);
    }

    public Object removeProperty(final String key) {
        return null == this.properties ? null : this.properties.remove(key);
    }

    public Object getProperty(final String key) {
        return null == this.properties ? null : this.properties.get(key);
    }

    public Set<String> getPropertyKeys() {
        return null == this.properties ? (Set) Collections.emptySet() : this.properties.keySet();
    }

    public Map<String, Object> getProperties() {
        return null == this.properties ? this.properties = new HashMap<String, Object>() : this.properties;
    }

    public void setProperties(final Map<String, Object> properties) {
        if (null == this.properties)
            this.properties = new HashMap<String, Object>();
        else
            this.properties.clear();
        for (final Map.Entry<String, Object> entry : properties.entrySet()) {
            this.properties.put(TYPE_MAP.get(entry.getKey()), entry.getValue());
        }
    }

    public Object getId() {
        return this.id;
    }

    public long getIdAsLong() {
        return this.id;
    }

    @Override
    public boolean equals(final Object other) {
        return this.getClass().equals(other.getClass()) && this.id == ((FaunusElement) other).getIdAsLong();
    }

    @Override
    public int hashCode() {
        return ((Long) this.id).hashCode();
    }

    public static class ElementProperties {

        public enum PropertyType {
            INT((byte) 0),
            LONG((byte) 1),
            FLOAT((byte) 2),
            DOUBLE((byte) 3),
            STRING((byte) 4);
            public byte val;

            private PropertyType(byte v) {
                this.val = v;
            }
        }

        public static void write(final Map<String, Object> properties, final DataOutput out) throws IOException {
            if (null == properties) {
                out.writeShort(0);
            } else {
                out.writeShort(properties.size());
                for (final Map.Entry<String, Object> entry : properties.entrySet()) {
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
        }

        public static Map<String, Object> readFields(final DataInput in) throws IOException {
            final int numberOfProperties = in.readShort();
            if (numberOfProperties == 0)
                return null;
            else {
                final Map<String, Object> properties = new HashMap<String, Object>();
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
                    properties.put(TYPE_MAP.get(key), valueObject);
                }
                return properties;
            }
        }

    }

    public static class ElementPaths {

        public static void write(final List<List<MicroElement>> paths, final DataOutput out) throws IOException {
            out.writeInt(paths.size());
            for (final List<MicroElement> path : paths) {
                out.writeInt(path.size());
                for (MicroElement element : path) {
                    if (element instanceof MicroVertex)
                        out.writeChar('v');
                    else
                        out.writeChar('e');
                    out.writeLong(element.getId());
                }
            }
        }

        public static List<List<MicroElement>> readFields(final DataInput in) throws IOException {
            int pathsSize = in.readInt();
            final List<List<MicroElement>> paths = new ArrayList<List<MicroElement>>(pathsSize);
            for (int i = 0; i < pathsSize; i++) {
                int pathSize = in.readInt();
                List<MicroElement> path = new ArrayList<MicroElement>(pathSize);
                for (int j = 0; j < pathSize; j++) {
                    char type = in.readChar();
                    if (type == 'v')
                        path.add(new MicroVertex(in.readLong()));
                    else
                        path.add(new MicroEdge(in.readLong()));
                }
                paths.add(path);
            }
            return paths;
        }

    }


}
