package com.thinkaurelius.titan.core.olap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Combiner<M> {

    public M combine(M m1, M m2);

}
