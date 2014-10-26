package com.thinkaurelius.titan.graphdb.transaction.indexcache;

import com.google.common.collect.HashMultimap;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanVertexProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConcurrentIndexCache implements IndexCache {

    private final HashMultimap<Object,TitanVertexProperty> map;

    public ConcurrentIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public synchronized void add(TitanVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public synchronized void remove(TitanVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public synchronized Iterable<TitanVertexProperty> get(final Object value, final PropertyKey key) {
        List<TitanVertexProperty> result = new ArrayList<TitanVertexProperty>(4);
        for (TitanVertexProperty p : map.get(value)) {
            if (p.propertyKey().equals(key)) result.add(p);
        }
        return result;
    }
}
