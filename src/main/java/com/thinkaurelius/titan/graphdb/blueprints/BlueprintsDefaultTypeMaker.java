package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.*;

/**
 *
 */
public class BlueprintsDefaultTypeMaker implements DefaultTypeMaker {

    public static final DefaultTypeMaker INSTANCE = new BlueprintsDefaultTypeMaker();

    private BlueprintsDefaultTypeMaker() {}

    @Override
    public TitanLabel makeLabel(String name, TypeMaker factory) {
        return factory.name(name).directed().makeEdgeLabel();
    }

    @Override
    public TitanKey makeKey(String name, TypeMaker factory) {
        return factory.name(name).functional(false).
                dataType(Object.class).makePropertyKey();
    }
}
