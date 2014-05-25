package com.thinkaurelius.titan.core.olap;

/**
 * Defines the state which a particular {@link OLAPJob} computes over. A state may be any mutable object that
 * carries the state of computation per vertex. It is required, that this object is {@link Cloneable} and
 * that we can merge states for different vertices. The merging may not depend on the order of the merge, in other words,
 * the operation must be commutative and associative.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface State<S extends State> {

    /**
     * Clones this state into a new (and independent) state
     * @return
     */
    public S clone();

    /**
     * Merges the other state into this state (i.e. mutates this state).
     * <p />
     * Merging is only required for graphs that have partitioned vertices so that the vertex state for each part of
     * a partitioned vertex can be merged together at the end. On graphs without such vertices, this method does not
     * need to be implemented and can throw {@link UnsupportedOperationException}.
     *
     * @param other state to merge in
     */
    public void merge(S other);

}
