package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractEdge extends AbstractTypedRelation implements TitanEdge {

    private final InternalVertex start;
    private final InternalVertex end;

    public AbstractEdge(long id, TitanLabel label, InternalVertex start, InternalVertex end) {
        super(id, label);
        Preconditions.checkNotNull(start);
        Preconditions.checkNotNull(end);
        this.start=start;
        this.end=end;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public String getLabel() {
        return type.getName();
    }

    @Override
    public InternalVertex getVertex(int pos) {
        if (pos==0) return start;
        else if (pos==1) return end;
        else throw new IllegalArgumentException("Invalid position: " + pos);
    }

    @Override
    public int getArity() {
        return 2;
    }

    @Override
    public int getLen() {
        return isUnidirected()?1:2;
    }

    @Override
    public TitanLabel getTitanLabel() {
        return (TitanLabel)type;
    }

    @Override
    public TitanVertex getVertex(Direction dir) {
        return getVertex(EdgeDirection.position(dir));
    }

    @Override
    public TitanVertex getOtherVertex(TitanVertex vertex) {
        if (start.equals(vertex)) return end;
        else if (end.equals(vertex)) return start;
        else throw new IllegalArgumentException("Edge is not incident on vertex");
    }

    @Override
    public boolean isDirected() {
        return !isUnidirected();
    }

    @Override
    public boolean isUnidirected() {
        return ((TitanLabel)type).isUnidirected();
    }

    @Override
    public boolean isProperty() {
        return false;
    }

    @Override
    public boolean isEdge() {
        return true;
    }
}
