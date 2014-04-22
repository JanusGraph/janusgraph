package com.thinkaurelius.titan.core;

/**
 * The cardinality of the values associated with given key for a particular element.
 *
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum Cardinality {

    /**
     * Only a single value may be associated with the given key.
     */
    SINGLE,

    /**
     * Multiple values and duplicate values may be associated with the given key.
     */
    LIST,


    /**
     * Multiple but distinct values may be associated with the given key.
     */
    SET;


}
