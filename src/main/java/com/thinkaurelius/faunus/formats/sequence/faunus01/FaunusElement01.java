package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

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
public abstract class FaunusElement01 implements Element, WritableComparable<FaunusElement01> {

    static {
        WritableComparator.define(FaunusElement01.class, new Comparator());
    }

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
        add(Boolean.class);
    }};


    protected long id;
    protected Map<String, Object> properties = null;
    protected List<List<MicroElement01>> paths = null;
    private MicroElement01 microVersion = null;
    protected boolean pathEnabled = false;
    protected long pathCounter = 0;


    public FaunusElement01(final long id) {
        this.id = id;
    }

    protected FaunusElement01 reuse(final long id) {
        this.id = id;
        this.properties = null;
        this.clearPaths();
        return this;
    }

    public void enablePath(final boolean enablePath) {
        this.pathEnabled = enablePath;
        if (this.pathEnabled) {
            if (null == this.microVersion)
                this.microVersion = (this instanceof FaunusVertex01) ? new FaunusVertex01.MicroVertex01(this.id) : new FaunusEdge01.MicroEdge01(this.id);
            if (null == this.paths)
                this.paths = new ArrayList<List<MicroElement01>>();
        }
        // TODO: else make pathCounter = paths.size()?
    }

    public void addPath(final List<MicroElement01> path, final boolean append) throws IllegalStateException {
        if (this.pathEnabled) {
            if (append) path.add(this.microVersion);
            this.paths.add(path);
        } else {
            throw new IllegalStateException("Path calculations are not enabled");
        }
    }

    public void addPaths(final List<List<MicroElement01>> paths, final boolean append) throws IllegalStateException {
        if (this.pathEnabled) {
            if (append) {
                for (final List<MicroElement01> path : paths) {
                    this.addPath(path, append);
                }
            } else
                this.paths.addAll(paths);
        } else {
            throw new IllegalStateException("Path calculations are not enabled");
        }
    }

    public List<List<MicroElement01>> getPaths() throws IllegalStateException {
        if (this.pathEnabled)
            return this.paths;
        else
            throw new IllegalStateException("Path calculations are not enabled");
    }

    public void getPaths(final FaunusElement01 element, final boolean append) {
        if (this.pathEnabled) {
            this.addPaths(element.getPaths(), append);
        } else {
            this.pathCounter = this.pathCounter + element.pathCount();
        }
    }

    public long incrPath(final long amount) throws IllegalStateException {
        if (this.pathEnabled)
            throw new IllegalStateException("Path calculations are enabled -- use addPath()");
        else
            this.pathCounter = this.pathCounter + amount;
        return this.pathCounter;
    }

    public boolean hasPaths() {
        if (this.pathEnabled)
            return !this.paths.isEmpty();
        else
            return this.pathCounter > 0;
    }

    public void clearPaths() {
        if (this.pathEnabled) {
            this.paths = new ArrayList<List<MicroElement01>>();
            this.microVersion = (this instanceof FaunusVertex01) ? new FaunusVertex01.MicroVertex01(this.id) : new FaunusEdge01.MicroEdge01(this.id);
        } else
            this.pathCounter = 0;
    }

    public long pathCount() {
        if (this.pathEnabled)
            return this.paths.size();
        else
            return this.pathCounter;
    }

    public void startPath() {
        if (this.pathEnabled) {
            this.clearPaths();
            final List<MicroElement01> startPath = new ArrayList<MicroElement01>();
            startPath.add(this.microVersion);
            this.paths.add(startPath);
        } else {
            this.pathCounter = 1;
        }
    }

    public void setProperty(final String key, final Object value) {
        ElementHelper.validateProperty(this, key, value);
        if (key.equals("_count"))
            throw new IllegalArgumentException("_count is a reserved property");

        if (null == this.properties)
            this.properties = new HashMap<String, Object>();
        this.properties.put(TYPE_MAP.get(key), value);
    }

    public <T> T removeProperty(final String key) {
        return null == this.properties ? null : (T) this.properties.remove(key);
    }

    public <T> T getProperty(final String key) {
        if (key.equals("_count"))
            return (T) Long.valueOf(this.pathCount());
        return null == this.properties ? null : (T) this.properties.get(key);
    }

    public Set<String> getPropertyKeys() {
        return null == this.properties ? (Set) Collections.emptySet() : this.properties.keySet();
    }

    public Map<String, Object> getProperties() {
        return null == this.properties ? this.properties = new HashMap<String, Object>() : this.properties;
    }

    public Object getId() {
        return this.id;
    }

    public long getIdAsLong() {
        return this.id;
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    public void readFields(final DataInput in) throws IOException {
        this.id = WritableUtils.readVLong(in);
        this.pathEnabled = in.readBoolean();
        if (this.pathEnabled) {
            this.paths = ElementPaths.readFields(in);
            this.microVersion = (this instanceof FaunusVertex01) ? new FaunusVertex01.MicroVertex01(this.id) : new FaunusEdge01.MicroEdge01(this.id);
        } else
            this.pathCounter = WritableUtils.readVLong(in);
        this.properties = ElementProperties.readFields(in);
    }

    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, this.id);
        out.writeBoolean(this.pathEnabled);
        if (this.pathEnabled)
            ElementPaths.write(this.paths, out);
        else
            WritableUtils.writeVLong(out, this.pathCounter);
        ElementProperties.write(this.properties, out);
    }

    @Override
    public boolean equals(final Object other) {
        return this.getClass().equals(other.getClass()) && this.id == ((FaunusElement01) other).getIdAsLong();
    }

    @Override
    public int hashCode() {
        return ((Long) this.id).hashCode();
    }

    public int compareTo(final FaunusElement01 other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }

    public static class ElementProperties {

        public enum PropertyType {
            INT((byte) 0),
            LONG((byte) 1),
            FLOAT((byte) 2),
            DOUBLE((byte) 3),
            STRING((byte) 4),
            BOOLEAN((byte) 5);
            public byte val;

            private PropertyType(byte v) {
                this.val = v;
            }
        }

        public static void write(final Map<String, Object> properties, final DataOutput out) throws IOException {
            if (null == properties) {
                WritableUtils.writeVInt(out, 0);
            } else {
                WritableUtils.writeVInt(out, properties.size());
                for (final Map.Entry<String, Object> entry : properties.entrySet()) {
                    out.writeUTF(entry.getKey());
                    final Class valueClass = entry.getValue().getClass();
                    final Object valueObject = entry.getValue();
                    if (valueClass.equals(Integer.class)) {
                        out.writeByte(PropertyType.INT.val);
                        WritableUtils.writeVInt(out, (Integer) valueObject);
                    } else if (valueClass.equals(Long.class)) {
                        out.writeByte(PropertyType.LONG.val);
                        WritableUtils.writeVLong(out, (Long) valueObject);
                    } else if (valueClass.equals(Float.class)) {
                        out.writeByte(PropertyType.FLOAT.val);
                        out.writeFloat((Float) valueObject);
                    } else if (valueClass.equals(Double.class)) {
                        out.writeByte(PropertyType.DOUBLE.val);
                        out.writeDouble((Double) valueObject);
                    } else if (valueClass.equals(String.class)) {
                        out.writeByte(PropertyType.STRING.val);
                        WritableUtils.writeString(out, (String) valueObject);
                    } else if (valueClass.equals(Boolean.class)) {
                        out.writeByte(PropertyType.BOOLEAN.val);
                        out.writeBoolean((Boolean) valueObject);
                    } else {
                        throw new IOException("Property value type of " + valueClass + " is not supported");
                    }
                }
            }
        }

        public static Map<String, Object> readFields(final DataInput in) throws IOException {
            final int numberOfProperties = WritableUtils.readVInt(in);
            if (numberOfProperties == 0)
                return null;
            else {
                final Map<String, Object> properties = new HashMap<String, Object>();
                for (int i = 0; i < numberOfProperties; i++) {
                    final String key = in.readUTF();
                    final byte valueClass = in.readByte();
                    final Object valueObject;
                    if (valueClass == PropertyType.INT.val) {
                        valueObject = WritableUtils.readVInt(in);
                    } else if (valueClass == PropertyType.LONG.val) {
                        valueObject = WritableUtils.readVLong(in);
                    } else if (valueClass == PropertyType.FLOAT.val) {
                        valueObject = in.readFloat();
                    } else if (valueClass == PropertyType.DOUBLE.val) {
                        valueObject = in.readDouble();
                    } else if (valueClass == PropertyType.STRING.val) {
                        valueObject = WritableUtils.readString(in);
                    } else if (valueClass == PropertyType.BOOLEAN.val) {
                        valueObject = in.readBoolean();
                    } else {
                        throw new IOException("Property value type of " + valueClass + " is not supported");
                    }
                    properties.put(TYPE_MAP.get(key), valueObject);
                }
                return properties;
            }
        }
    }

    protected static class ElementPaths {

        public static void write(final List<List<MicroElement01>> paths, final DataOutput out) throws IOException {
            if (null == paths) {
                WritableUtils.writeVInt(out, 0);
            } else {
                WritableUtils.writeVInt(out, paths.size());
                for (final List<MicroElement01> path : paths) {
                    WritableUtils.writeVInt(out, path.size());
                    for (MicroElement01 element : path) {
                        if (element instanceof FaunusVertex01.MicroVertex01)
                            out.writeChar('v');
                        else
                            out.writeChar('e');
                        WritableUtils.writeVLong(out, element.getId());
                    }
                }
            }
        }

        public static List<List<MicroElement01>> readFields(final DataInput in) throws IOException {
            int pathsSize = WritableUtils.readVInt(in);
            if (pathsSize == 0)
                return new ArrayList<List<MicroElement01>>();
            else {
                final List<List<MicroElement01>> paths = new ArrayList<List<MicroElement01>>(pathsSize);
                for (int i = 0; i < pathsSize; i++) {
                    int pathSize = WritableUtils.readVInt(in);
                    final List<MicroElement01> path = new ArrayList<MicroElement01>(pathSize);
                    for (int j = 0; j < pathSize; j++) {
                        char type = in.readChar();
                        if (type == 'v')
                            path.add(new FaunusVertex01.MicroVertex01(WritableUtils.readVLong(in)));
                        else
                            path.add(new FaunusEdge01.MicroEdge01(WritableUtils.readVLong(in)));
                    }
                    paths.add(path);
                }
                return paths;
            }
        }

    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FaunusElement01.class);
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
            if (a instanceof FaunusElement01 && b instanceof FaunusElement01)
                return ((Long) (((FaunusElement01) a).getIdAsLong())).compareTo(((FaunusElement01) b).getIdAsLong());
            else
                return super.compare(a, b);
        }
    }

    public static abstract class MicroElement01 {

        protected final long id;

        public MicroElement01(final long id) {
            this.id = id;
        }

        public long getId() {
            return this.id;
        }

        public int hashCode() {
            return Long.valueOf(this.id).hashCode();
        }

        public boolean equals(final Object object) {
            return (object.getClass().equals(this.getClass()) && this.id == ((MicroElement01) object).getId());
        }
    }
}