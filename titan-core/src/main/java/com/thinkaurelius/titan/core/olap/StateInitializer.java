package com.thinkaurelius.titan.core.olap;

/**
 * Returns the initial state for a vertex.
 * </p>
 * It is important to return independent objects unless they are immutable. Otherwise data inconsistencies will arise.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface StateInitializer<S extends State<S>> {

    public S initialState();


}
