package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.*;

/**
 * {@link com.thinkaurelius.titan.core.DefaultSchemaMaker} implementation for Blueprints graphs
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BlueprintsDefaultSchemaMaker implements DefaultSchemaMaker {

    public static final DefaultSchemaMaker INSTANCE = new BlueprintsDefaultSchemaMaker();

    private BlueprintsDefaultSchemaMaker() {
    }

    @Override
    public EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
        return factory.directed().make();
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        return factory.dataType(Object.class).make();
    }

    @Override
    public VertexLabel makeVertexLabel(VertexLabelMaker factory) {
        return factory.make();
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return true;
    }
}
