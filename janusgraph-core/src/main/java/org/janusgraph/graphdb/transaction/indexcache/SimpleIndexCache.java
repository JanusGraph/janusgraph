package org.janusgraph.graphdb.transaction.indexcache;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusVertexProperty;

import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleIndexCache implements IndexCache {

    private final HashMultimap<Object,JanusVertexProperty> map;

    public SimpleIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public void add(JanusVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public void remove(JanusVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public Iterable<JanusVertexProperty> get(final Object value, final PropertyKey key) {
        return Iterables.filter(map.get(value),new Predicate<JanusVertexProperty>() {
            @Override
            public boolean apply(@Nullable JanusVertexProperty janusProperty) {
                return janusProperty.propertyKey().equals(key);
            }
        });
    }
}
