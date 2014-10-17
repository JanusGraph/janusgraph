package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.tinkerpop.gremlin.structure.Element;

import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleTitanProperty<V> implements TitanProperty<V> {

    private final InternalRelationType type;
    private final V value;
    private final TitanElement element;

    public SimpleTitanProperty(TitanElement element, RelationType type, V value) {
        this.type = (InternalRelationType)type;
        this.value = value;
        this.element = element;
    }

    @Override
    public RelationType getType() {
        return type;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public boolean isHidden() {
        return ((InternalRelationType) type).isHiddenType();
    }

    @Override
    public <E extends Element> E getElement() {
        return (E)element;
    }

    @Override
    public void remove() {
        element.removeProperty(type);
    }
}
