package com.thinkaurelius.titan.graphdb.edgetypes;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public enum FunctionalType {
    
    NON_FUNCTIONAL, FUNCTIONAL, FUNCTIONAL_LOCKING;
    
    
    public boolean isFunctional() {
        return this==FUNCTIONAL || this==FUNCTIONAL_LOCKING;
    }
    
    public boolean isLocking() {
        return this==FUNCTIONAL_LOCKING;
    }
    
    
}
