package com.thinkaurelius.titan.util.datastructures;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PointInterval<T> implements Interval<T> {

    private T point=null;

    public PointInterval(T point) {
        this.point=point;
    }

    public T getPoint() {
        return point;
    }

    public void setPoint(T point) {
        this.point=point;
    }


    @Override
    public T getStart() {
        return point;
    }

    @Override
    public T getEnd() {
        return point;
    }

    @Override
    public boolean startInclusive() {
        return true;
    }

    @Override
    public boolean endInclusive() {
        return true;
    }

    @Override
    public boolean isPoint() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(point).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        PointInterval oth = (PointInterval)other;
        if (point==null ^ oth.point==null) return false;
        return point.equals(oth.point);
    }

    @Override
    public String toString() {
        return "["+point+"]";
    }
}
