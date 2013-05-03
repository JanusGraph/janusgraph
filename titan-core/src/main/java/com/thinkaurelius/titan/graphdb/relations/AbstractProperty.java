package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import static com.tinkerpop.blueprints.util.StringFactory.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
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
    public String toString() {
        String valueStr = String.valueOf(value);
        valueStr = valueStr.substring(0,Math.min(valueStr.length(),20));
        return E + L_BRACKET + getId() + R_BRACKET + L_BRACKET + getVertex().getId() + DASH + getPropertyKey() + ARROW + valueStr + R_BRACKET;
    }

    @Override
    public InternalVertex getVertex(int pos) {
        if (pos==0) return vertex;
        else throw new IllegalArgumentException("Invalid position: " + pos);
    }

    @Override
    public final int getArity() {
        return 1;
    }

    @Override
    public final int getLen() {
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
