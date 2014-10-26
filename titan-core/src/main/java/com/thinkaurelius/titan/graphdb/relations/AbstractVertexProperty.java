package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import static com.tinkerpop.gremlin.structure.util.StringFactory.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractVertexProperty<V> extends AbstractTypedRelation implements TitanVertexProperty<V> {

    private InternalVertex vertex;
    private final Object value;

    public AbstractVertexProperty(long id, PropertyKey type, InternalVertex vertex, Object value) {
        super(id, type);
        Preconditions.checkNotNull(vertex, "null vertex");
        Preconditions.checkNotNull(value, "null value for property key %s",type);
        this.vertex=vertex;
        this.value=value;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
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
    public TitanVertex getElement() {
        return vertex;
    }

    @Override
    public V value() {
        return (V)value;
    }

    @Override
    public boolean isProperty() {
        return true;
    }

    @Override
    public boolean isEdge() {
        return false;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this;
    }

}
