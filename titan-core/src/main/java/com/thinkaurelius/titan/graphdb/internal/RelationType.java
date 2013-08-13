package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum RelationType implements Condition<TitanRelation> {

    EDGE, PROPERTY, RELATION;

    public boolean isProper() {
        switch(this) {
            case EDGE:
            case PROPERTY:
                return true;
            case RELATION:
                return false;
            default: throw new AssertionError("Unrecognized type: " + this);
        }
    }

    /*
    ########### CONDITION DEFINITION #################
     */

    @Override
    public Type getType() {
        return Type.LITERAL;
    }

    @Override
    public Iterable getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean hasChildren() {
        return false;
    }

    @Override
    public int numChildren() {
        return 0;
    }

    @Override
    public boolean evaluate(TitanRelation relation) {
        switch(this) {
            case EDGE:
                return relation.isEdge();
            case PROPERTY:
                return relation.isProperty();
            case RELATION:
                return true;
            default: throw new AssertionError("Unrecognized type: " + this);
        }
    }
}
