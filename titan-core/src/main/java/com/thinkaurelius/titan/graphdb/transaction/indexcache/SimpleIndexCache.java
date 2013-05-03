package com.thinkaurelius.titan.graphdb.transaction.indexcache;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;

import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleIndexCache implements IndexCache {

    private final HashMultimap<Object,TitanProperty> map;

    public SimpleIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public void add(TitanProperty property) {
        map.put(property.getValue(),property);
    }

    @Override
    public void remove(TitanProperty property) {
        map.remove(property.getValue(),property);
    }

    @Override
    public Iterable<TitanProperty> get(final Object value, final TitanKey key) {
        return Iterables.filter(map.get(value),new Predicate<TitanProperty>() {
            @Override
            public boolean apply(@Nullable TitanProperty titanProperty) {
                return titanProperty.getPropertyKey().equals(key);
            }
        });
    }
}
