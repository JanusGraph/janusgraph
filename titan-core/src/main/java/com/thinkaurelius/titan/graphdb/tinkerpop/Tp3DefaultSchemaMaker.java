package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.DefaultSchemaMaker;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Tp3DefaultSchemaMaker implements DefaultSchemaMaker {

    public static final DefaultSchemaMaker INSTANCE = new Tp3DefaultSchemaMaker();

    private Tp3DefaultSchemaMaker() {
    }

    @Override
    public EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
        return factory.directed().make();
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        return factory.cardinality(Cardinality.LIST).dataType(Object.class).make();
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
