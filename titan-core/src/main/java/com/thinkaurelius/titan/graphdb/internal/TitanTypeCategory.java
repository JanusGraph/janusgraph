package com.thinkaurelius.titan.graphdb.internal;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum TitanTypeCategory {

    LABEL, KEY, INDEX, MODIFIER;


    public boolean isRelationType() {
        return this==LABEL || this==KEY;
    }


}
