package com.thinkaurelius.faunus.mapreduce.algebra.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Function<A, B> {

    public B compute(A a);
}
