package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleFaunusEdge extends SimpleFaunusRelation implements FaunusEdge {

    private final FaunusEdgeLabel label;
    private final HadoopVertex vertex;

    public SimpleFaunusEdge(FaunusEdgeLabel label, HadoopVertex vertex) {
        this.label = label;
        this.vertex = vertex;
    }

    @Override
    protected Object otherValue() {
        return vertex;
    }

    @Override
    public EdgeLabel getEdgeLabel() {
        return label;
    }

    @Override
    public TitanVertex getVertex(Direction dir) {
        if (dir!=Direction.IN) throw new UnsupportedOperationException();
        return vertex;
    }

    @Override
    public String getLabel() {
        return label.getName();
    }

    @Override
    public TitanVertex getOtherVertex(TitanVertex vertex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDirected() {
        return label.isDirected();
    }

    @Override
    public boolean isUnidirected() {
        return label.isUnidirected();
    }

    @Override
    public FaunusRelationType getType() {
        return label;
    }
}
