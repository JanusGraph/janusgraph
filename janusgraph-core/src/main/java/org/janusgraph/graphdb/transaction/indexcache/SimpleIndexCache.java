package org.janusgraph.graphdb.transaction.indexcache;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphVertexProperty;

import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleIndexCache implements IndexCache {

    private final HashMultimap<Object,JanusGraphVertexProperty> map;

    public SimpleIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public void add(JanusGraphVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public void remove(JanusGraphVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public Iterable<JanusGraphVertexProperty> get(final Object value, final PropertyKey key) {
        return Iterables.filter(map.get(value),new Predicate<JanusGraphVertexProperty>() {
            @Override
            public boolean apply(@Nullable JanusGraphVertexProperty janusgraphProperty) {
                return janusgraphProperty.propertyKey().equals(key);
            }
        });
    }
}
