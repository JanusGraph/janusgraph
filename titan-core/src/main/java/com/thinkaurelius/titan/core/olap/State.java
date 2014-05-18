package com.thinkaurelius.titan.core.olap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface State<S extends State> {

    /**
     * Clones this state into a new (and independent) state
     * @return
     */
    public S clone();

    /**
     * Merges the other state into this state
     *
     * @param other
     */
    public void merge(S other);

}
