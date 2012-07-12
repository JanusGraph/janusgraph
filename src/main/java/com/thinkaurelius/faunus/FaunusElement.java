package com.thinkaurelius.faunus;

import com.tinkerpop.blueprints.Element;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement<T extends Element> implements Element, WritableComparable<T> {

    protected Map<String, Object> properties = new HashMap<String, Object>();
    protected long id;
    protected LongWritable writableId;

    public enum ElementType {
        VERTEX((byte) 0),
        EDGE((byte) 1);
        public byte val;

        private ElementType(byte v) {
            this.val = v;
        }
    }

    public FaunusElement(final Long id) {
        this.id = id;
        this.writableId = new LongWritable(this.id);
    }

    public void setProperty(final String key, final Object value) {
        this.properties.put(key, value);
    }

    public Object removeProperty(final String key) {
        return this.properties.remove(key);
    }

    public Object getProperty(final String key) {
        return this.properties.get(key);
    }

    public Set<String> getPropertyKeys() {
        return this.properties.keySet();
    }

    public Map<String, Object> getProperties() {
        return this.properties;
    }

    public void setProperties(final Map<String, Object> properties) {
        this.properties.clear();
        for (final Map.Entry<String, Object> entry : properties.entrySet()) {
            this.properties.put(entry.getKey(), entry.getValue());
        }
    }

    public Object getId() {
        return this.id;
    }

    public Long getIdAsLong() {
        return this.id;
    }

    public LongWritable getIdAsLongWritable() {
        return new LongWritable(this.id);
    }

    public int compareTo(final T other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }

    @Override
    public boolean equals(final Object other) {
        return this.getClass().equals(other.getClass()) && this.id == (Long) ((FaunusElement) other).getId();
    }

    @Override
    public int hashCode() {
        return ((Long) this.id).hashCode();
    }
}
