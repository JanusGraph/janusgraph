package com.thinkaurelius.titan.graphdb.transaction.indexcache;

import com.google.common.collect.HashMultimap;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConcurrentIndexCache implements IndexCache {

    private final HashMultimap<Object,TitanProperty> map;

    public ConcurrentIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public synchronized void add(TitanProperty property) {
        map.put(property.getValue(),property);
    }

    @Override
    public synchronized void remove(TitanProperty property) {
        map.remove(property.getValue(),property);
    }

    @Override
    public synchronized Iterable<TitanProperty> get(final Object value, final TitanKey key) {
        List<TitanProperty> result = new ArrayList<TitanProperty>(4);
        for (TitanProperty p : map.get(value)) {
            if (p.getPropertyKey().equals(key)) result.add(p);
        }
        return result;
    }
}
