package com.thinkaurelius.titan.graphdb.internal;

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

    /**
     * Modulates the result of a {@link Comparable#compareTo(Object)} execution for this specific
     * order, i.e. it negates the result if the order is {@link #DESC}.
     *
     * @param compare
     * @return
     */
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

    /**
     * The default order when none is specified
     */
    public static final Order DEFAULT = ASC;

    public org.apache.tinkerpop.gremlin.process.traversal.Order getTP() {
        switch (this) {
            case ASC :return org.apache.tinkerpop.gremlin.process.traversal.Order.incr;
            case DESC: return org.apache.tinkerpop.gremlin.process.traversal.Order.decr;
            default: throw new AssertionError();
        }
    }

    public static Order convert(org.apache.tinkerpop.gremlin.process.traversal.Order order) {
        switch(order) {
            case incr: return ASC;
            case decr: return DESC;
            default: throw new AssertionError();
        }
    }

}
