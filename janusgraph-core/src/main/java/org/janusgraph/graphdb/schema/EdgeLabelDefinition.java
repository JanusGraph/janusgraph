package org.janusgraph.graphdb.schema;

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.apache.tinkerpop.gremlin.structure.Direction;

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
        this(label.name(),label.longId(),label.multiplicity(),label.isUnidirected());
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
