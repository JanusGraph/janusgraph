package com.thinkaurelius.titan.graphdb.edgetypes;

import com.thinkaurelius.titan.core.*;

/**
 * Created by IntelliJ IDEA.
 * User: matthias
 * Date: 3/5/12
 * Time: 3:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class StandardDefaultEdgeTypeMaker implements DefaultEdgeTypeMaker {

    public static final DefaultEdgeTypeMaker instance = new StandardDefaultEdgeTypeMaker();

    private StandardDefaultEdgeTypeMaker() {}

    @Override
    public RelationshipType makeRelationshipType(String name, EdgeTypeMaker factory) {
        return factory.withName(name).withDirectionality(Directionality.Directed).category(EdgeCategory.Labeled).makeRelationshipType();
    }

    @Override
    public PropertyType makePropertyType(String name, EdgeTypeMaker factory) {
        return factory.withName(name).category(EdgeCategory.Simple).functional(true).
                setIndex(PropertyIndex.None).
                dataType(Object.class).makePropertyType();
    }
}
