package com.thinkaurelius.titan.core;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanProperty<V> extends Property<V> {

    public RelationType getType();

    @Override
    public default String key() {
        return Graph.Key.unHide(getType().name());
    }

}
