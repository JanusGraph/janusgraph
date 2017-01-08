package org.janusgraph.graphdb.transaction.indexcache;

import com.google.common.collect.HashMultimap;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphVertexProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConcurrentIndexCache implements IndexCache {

    private final HashMultimap<Object,JanusGraphVertexProperty> map;

    public ConcurrentIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public synchronized void add(JanusGraphVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public synchronized void remove(JanusGraphVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public synchronized Iterable<JanusGraphVertexProperty> get(final Object value, final PropertyKey key) {
        List<JanusGraphVertexProperty> result = new ArrayList<JanusGraphVertexProperty>(4);
        for (JanusGraphVertexProperty p : map.get(value)) {
            if (p.propertyKey().equals(key)) result.add(p);
        }
        return result;
    }
}
