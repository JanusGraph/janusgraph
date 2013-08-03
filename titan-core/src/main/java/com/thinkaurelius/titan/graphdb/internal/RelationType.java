package com.thinkaurelius.titan.graphdb.internal;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum RelationType {

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

}
