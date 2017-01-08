package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphProperty<V> extends Property<V> {

    public PropertyKey propertyKey();

    @Override
    public default String key() {
        return propertyKey().name();
    }

}
