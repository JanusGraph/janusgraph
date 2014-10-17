package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanElement;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanProperty<V> extends Property<V> {

    public RelationType getType();

    @Override
    public default String key() {
        return getType().getName();
    }

}
