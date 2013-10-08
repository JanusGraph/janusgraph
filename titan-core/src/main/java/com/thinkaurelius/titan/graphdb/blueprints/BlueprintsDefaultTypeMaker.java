package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.*;
import com.tinkerpop.blueprints.Direction;

/**
 * {@link DefaultTypeMaker} implementation for Blueprints graphs
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BlueprintsDefaultTypeMaker implements DefaultTypeMaker {

    public static final DefaultTypeMaker INSTANCE = new BlueprintsDefaultTypeMaker();

    private BlueprintsDefaultTypeMaker() {
    }

    @Override
    public TitanLabel makeLabel(LabelMaker factory) {
        return factory.directed().make();
    }

    @Override
    public TitanKey makeKey(KeyMaker factory) {
        return factory.dataType(Object.class).make();
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return true;
    }
}
