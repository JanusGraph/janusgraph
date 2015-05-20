package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.DefaultSchemaMaker;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;

/**
 * {@link com.thinkaurelius.titan.core.schema.DefaultSchemaMaker} implementation for Blueprints graphs
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanDefaultSchemaMaker implements DefaultSchemaMaker {

    public static final DefaultSchemaMaker INSTANCE = new TitanDefaultSchemaMaker();

    private TitanDefaultSchemaMaker() {
    }

    @Override
    public Cardinality defaultPropertyCardinality(String key) {
        return Cardinality.SINGLE;
    }


    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return true;
    }
}
