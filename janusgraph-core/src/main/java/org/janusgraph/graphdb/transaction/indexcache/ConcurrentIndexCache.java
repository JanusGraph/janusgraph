package org.janusgraph.graphdb.transaction.indexcache;

import com.google.common.collect.HashMultimap;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusVertexProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConcurrentIndexCache implements IndexCache {

    private final HashMultimap<Object,JanusVertexProperty> map;

    public ConcurrentIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public synchronized void add(JanusVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public synchronized void remove(JanusVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public synchronized Iterable<JanusVertexProperty> get(final Object value, final PropertyKey key) {
        List<JanusVertexProperty> result = new ArrayList<JanusVertexProperty>(4);
        for (JanusVertexProperty p : map.get(value)) {
            if (p.propertyKey().equals(key)) result.add(p);
        }
        return result;
    }
}
