package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import static com.tinkerpop.blueprints.util.StringFactory.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractProperty extends AbstractTypedRelation implements TitanProperty {

    private InternalVertex vertex;
    private final Object value;

    public AbstractProperty(long id, PropertyKey type, InternalVertex vertex, Object value) {
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

    public void setVertexAt(int pos, InternalVertex vertex) {
        Preconditions.checkArgument(pos==0 && vertex!=null && this.vertex.equals(vertex));
        this.vertex=vertex;
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
    public PropertyKey getPropertyKey() {
        return (PropertyKey)type;
    }

    @Override
    public TitanVertex getVertex() {
        return vertex;
    }

    @Override
    public<O> O getValue() {
        return (O)value;
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
