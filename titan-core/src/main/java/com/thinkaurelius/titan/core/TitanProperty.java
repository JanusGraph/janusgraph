package com.thinkaurelius.titan.core;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanProperty<V> extends Property<V> {

    public RelationType getType();

    @Override
    public default String key() {
        return getType().name();
    }

}
