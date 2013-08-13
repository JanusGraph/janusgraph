package com.thinkaurelius.titan.util.datastructures;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface Interval<T> {

    public T getStart();

    public T getEnd();

    public boolean startInclusive();

    public boolean endInclusive();

    public boolean isPoint();

    public boolean isEmpty();

}
