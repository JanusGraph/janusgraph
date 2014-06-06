package com.thinkaurelius.titan.core.olap;

/**
 * Combines two states into one combined state.
 * <p/>
 * Used to combine the gathered states in an {@link OLAPQueryBuilder} over the entire adjacency list of a central
 * vertex into a single state.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Combiner<M> {

    /**
     * Combines two state into a combined state.
     *
     * @param m1
     * @param m2
     * @return The combination of m1 and m2
     */
    public M combine(M m1, M m2);

}
