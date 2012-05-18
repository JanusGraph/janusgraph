package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.*;

/**
 *
 */
public class StandardDefaultTypeMaker implements DefaultTypeMaker {

    public static final DefaultTypeMaker INSTANCE = new StandardDefaultTypeMaker();

    private StandardDefaultTypeMaker() {}

    @Override
    public TitanLabel makeRelationshipType(String name, TypeMaker factory) {
        return factory.name(name).directed().makeEdgeLabel();
    }

    @Override
    public TitanKey makePropertyType(String name, TypeMaker factory) {
        return factory.name(name).indexed().
                dataType(Object.class).makePropertyKey();
    }
}
