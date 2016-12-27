package com.thinkaurelius.titan.graphdb.types.typemaker;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.DefaultSchemaMaker;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DisableDefaultSchemaMaker implements DefaultSchemaMaker {

    public static final DefaultSchemaMaker INSTANCE = new DisableDefaultSchemaMaker();

    private DisableDefaultSchemaMaker() {
    }

    @Override
    public EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
        throw new IllegalArgumentException("Edge Label with given name does not exist: " + factory.getName());
    }

    @Override
    public Cardinality defaultPropertyCardinality(String key) {
        return Cardinality.SINGLE;
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        throw new IllegalArgumentException("Property Key with given name does not exist: " + factory.getName());
    }

    @Override
    public VertexLabel makeVertexLabel(VertexLabelMaker factory) {
        throw new IllegalArgumentException("Vertex Label with given name does not exist: " + factory.getName());
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return false;
    }
}
