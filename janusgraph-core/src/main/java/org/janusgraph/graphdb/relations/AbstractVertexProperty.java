package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

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
    public TitanTransaction graph() {
        return vertex.graph();
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return super.properties(propertyKeys);
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
    public TitanVertex element() {
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

}
