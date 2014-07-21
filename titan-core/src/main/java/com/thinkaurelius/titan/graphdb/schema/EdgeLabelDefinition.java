package com.thinkaurelius.titan.graphdb.schema;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class EdgeLabelDefinition extends RelationTypeDefinition {

    private final boolean unidirected;

    public EdgeLabelDefinition(String name, long id, Multiplicity multiplicity, boolean unidirected) {
        super(name, id, multiplicity);
        this.unidirected = unidirected;
    }

    public EdgeLabelDefinition(EdgeLabel label) {
        this(label.getName(),label.getLongId(),label.getMultiplicity(),label.isUnidirected());
    }

    public boolean isDirected() {
        return !unidirected;
    }

    public boolean isUnidirected() {
        return unidirected;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        if (unidirected) return dir==Direction.OUT;
        else return dir==Direction.BOTH;
    }



}
