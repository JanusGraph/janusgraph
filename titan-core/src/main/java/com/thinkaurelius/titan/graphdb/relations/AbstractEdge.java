package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractEdge extends AbstractTypedRelation implements TitanEdge {

    private InternalVertex start;
    private InternalVertex end;

    public AbstractEdge(long id, EdgeLabel label, InternalVertex start, InternalVertex end) {
        super(id, label);

        assert start != null && end != null;
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public String label() {
        return type.name();
    }

    public void setVertexAt(int pos, InternalVertex vertex) {
        Preconditions.checkArgument(vertex != null && getVertex(pos).equals(vertex));
        switch (pos) {
            case 0:
                start = vertex;
                break;
            case 1:
                end = vertex;
                break;
            default:
                throw new IllegalArgumentException("Invalid position: " + pos);
        }
    }

    @Override
    public InternalVertex getVertex(int pos) {
        switch (pos) {
            case 0:
                return start;
            case 1:
                return end;
            default:
                throw new IllegalArgumentException("Invalid position: " + pos);
        }
    }

    @Override
    public int getArity() {
        return 2;
    }

    @Override
    public int getLen() {
        assert !type.isUnidirected(Direction.IN);
        return type.isUnidirected(Direction.OUT)?1:2;
    }

    @Override
    public TitanVertex vertex(Direction dir) {
        return getVertex(EdgeDirection.position(dir));
    }


    @Override
    public TitanVertex otherVertex(Vertex vertex) {
        if (start.equals(vertex))
            return end;

        if (end.equals(vertex))
            return start;

        throw new IllegalArgumentException("Edge is not incident on vertex");
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
