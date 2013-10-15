package com.thinkaurelius.titan.core;

/**
 * Constants to specify the ordering of a result set in queries.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Order {

    /**
     * Increasing
     */
    ASC,
    /**
     * Decreasing
     */
    DESC;

    public int modulateNaturalOrder(int compare) {
        switch (this) {
            case ASC:
                return compare;
            case DESC:
                return -compare;
            default:
                throw new AssertionError("Unrecognized order: " + this);
        }
    }

}
