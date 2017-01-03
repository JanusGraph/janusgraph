package org.janusgraph.graphdb.tinkerpop;

import org.janusgraph.core.*;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.VertexLabelMaker;

/**
 * {@link org.janusgraph.core.schema.DefaultSchemaMaker} implementation for Blueprints graphs
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusDefaultSchemaMaker implements DefaultSchemaMaker {

    public static final DefaultSchemaMaker INSTANCE = new JanusDefaultSchemaMaker();

    private JanusDefaultSchemaMaker() {
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
