package com.thinkaurelius.titan.util.datastructures;

import java.io.Serializable;

/**
 * A counter with a long value
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class LongCounter implements Serializable {

    private static final long serialVersionUID = -880751358315110930L;


    private long count;

    public LongCounter(long initial) {
        count = initial;
    }

    public LongCounter() {
        this(0);
    }

    public void increment(long delta) {
        count += delta;
    }

    public void decrement(long delta) {
        count -= delta;
    }

    public void set(long value) {
        count = value;
    }

    public long get() {
        return count;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other;
    }

}
