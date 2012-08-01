package com.thinkaurelius.faunus.mapreduce;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Function<A, B> {

    public B compute(A a);
}
