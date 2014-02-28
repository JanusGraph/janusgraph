package com.thinkaurelius.titan.core.olap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface StateInitializer<S> {

    public S initialState();


}
