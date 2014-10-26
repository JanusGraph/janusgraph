package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleFaunusEdge extends SimpleFaunusRelation implements FaunusEdge {

    private final FaunusEdgeLabel label;
    private final FaunusVertex vertex;

    public SimpleFaunusEdge(FaunusEdgeLabel label, FaunusVertex vertex) {
        Preconditions.checkArgument(label.isUnidirected(),"Invalid edge label: %s",label);
        this.label = label;
        this.vertex = vertex;
    }

    @Override
    protected Object otherValue() {
        return vertex;
    }

    @Override
    public TitanVertex getVertex(Direction dir) {
        if (dir!=Direction.IN) throw new UnsupportedOperationException();
        return vertex;
    }

    @Override
    public long getVertexId(Direction dir) {
        if (dir!=Direction.IN) throw new UnsupportedOperationException();
        return vertex.longId();
    }

    @Override
    public String getLabel() {
        return label.name();
    }

    @Override
    public TitanVertex otherVertex(TitanVertex vertex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FaunusRelationType getType() {
        return label;
    }

}
