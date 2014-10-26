package com.thinkaurelius.titan.graphdb.transaction.indexcache;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanVertexProperty;

import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleIndexCache implements IndexCache {

    private final HashMultimap<Object,TitanVertexProperty> map;

    public SimpleIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public void add(TitanVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public void remove(TitanVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public Iterable<TitanVertexProperty> get(final Object value, final PropertyKey key) {
        return Iterables.filter(map.get(value),new Predicate<TitanVertexProperty>() {
            @Override
            public boolean apply(@Nullable TitanVertexProperty titanProperty) {
                return titanProperty.propertyKey().equals(key);
            }
        });
    }
}
