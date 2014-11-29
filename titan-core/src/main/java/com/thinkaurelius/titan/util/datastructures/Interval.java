package com.thinkaurelius.titan.util.datastructures;

import java.util.Collection;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface Interval<T> {

    public T getStart();

    public T getEnd();

    public boolean startInclusive();

    public boolean endInclusive();

    public boolean isPoints();

    public Collection<T> getPoints();

    public boolean isEmpty();

    public Interval<T> intersect(Interval<T> other);

}
