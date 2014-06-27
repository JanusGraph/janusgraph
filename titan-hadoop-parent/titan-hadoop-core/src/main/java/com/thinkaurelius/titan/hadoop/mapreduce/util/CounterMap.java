package com.thinkaurelius.titan.hadoop.mapreduce.util;

import java.util.HashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CounterMap<A> extends HashMap<A, Long> {

    public void incr(final A a, final long amount) {
        final Long count = this.get(a);
        if (null == count) {
            this.put(a, amount);
        } else {
            this.put(a, amount + count);
        }
    }

}
