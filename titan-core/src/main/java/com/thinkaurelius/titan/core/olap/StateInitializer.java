package com.thinkaurelius.titan.core.olap;

/**
 * Returns the initial state for a vertex.
 * </p>
 * It is important to return independent objects unless they are immutable. Otherwise data inconsistencies will arise.
 * Furthermore, it is expected that {@link #initialState()} will return the same state on subsequent invocations because
 * the state is not cached and hence this method may be called multiple times for the same vertex.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface StateInitializer<S> {

    public S initialState();


}
