package com.thinkaurelius.faunus.graph.io;

import com.tinkerpop.blueprints.pgm.Element;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement<T extends Element> implements Element, WritableComparable<T> {

    private Map<String, Object> properties = new HashMap<String, Object>();
    protected long id;

    public enum Type {
        VERTEX((byte) 0),
        EDGE((byte) 1);
        public byte val;

        private Type(byte v) {
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

    public int compareTo(final T other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }
}
