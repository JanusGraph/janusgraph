package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TypeMaker;
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
    public TitanLabel makeLabel(String name, TypeMaker factory) {
        return factory.name(name).directed().makeEdgeLabel();
    }

    @Override
    public TitanKey makeKey(String name, TypeMaker factory) {
        return factory.name(name).dataType(Object.class).makePropertyKey();
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return true;
    }
}
