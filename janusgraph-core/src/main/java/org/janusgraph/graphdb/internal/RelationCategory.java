package org.janusgraph.graphdb.internal;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.janusgraph.core.JanusRelation;
import org.janusgraph.core.JanusVertexQuery;
import org.janusgraph.graphdb.query.condition.Condition;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum RelationCategory implements Condition<JanusRelation> {

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

    public Iterable<JanusRelation> executeQuery(JanusVertexQuery query) {
        switch (this) {
            case EDGE: return (Iterable)query.edges();
            case PROPERTY: return (Iterable)query.properties();
            case RELATION: return query.relations();
            default: throw new AssertionError();
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
    public boolean evaluate(JanusRelation relation) {
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
