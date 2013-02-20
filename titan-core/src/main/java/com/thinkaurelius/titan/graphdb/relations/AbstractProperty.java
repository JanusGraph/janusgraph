package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractProperty extends AbstractTypedRelation implements TitanProperty {

    private final InternalVertex vertex;
    private final Object value;

    public AbstractProperty(long id, TitanKey type, InternalVertex vertex, Object value) {
        super(id, type);
        Preconditions.checkNotNull(vertex);
        Preconditions.checkNotNull(value);
        this.vertex=vertex;
        this.value=value;
    }

    @Override
    public InternalVertex getVertex(int pos) {
        if (pos==0) return vertex;
        else throw new IllegalArgumentException("Invalid position: " + pos);
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public TitanKey getPropertyKey() {
        return (TitanKey)type;
    }

    @Override
    public TitanVertex getVertex() {
        return vertex;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public <O> O getValue(Class<O> clazz) {
        return clazz.cast(value);
    }

    @Override
    public boolean isProperty() {
        return true;
    }

    @Override
    public boolean isEdge() {
        return false;
    }
}
