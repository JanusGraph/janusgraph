package com.thinkaurelius.titan.util.datastructures;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ProperInterval<T extends Comparable<T>> implements Interval<T> {

    private boolean startInclusive=true;
    private boolean endInclusive=true;
    private T start=null;
    private T end=null;

    public ProperInterval() {

    }

    public ProperInterval(T start, T end) {
        setStart(start);
        setEnd(end);
    }

    public void setStart(T start) {
        this.start=start;
    }

    public void setEnd(T end) {
        this.end=end;
    }

    public void setStartInclusive(boolean inclusive) {
        this.startInclusive=inclusive;
    }

    public void setEndInclusive(boolean inclusive) {
        this.endInclusive=inclusive;
    }

    @Override
    public T getStart() {
        return start;
    }

    @Override
    public T getEnd() {
        return end;
    }

    @Override
    public boolean startInclusive() {
        return startInclusive;
    }

    @Override
    public boolean endInclusive() {
        return endInclusive;
    }

    @Override
    public boolean isPoint() {
        return start!=null && end!=null && start.equals(end) && startInclusive && endInclusive;
    }

    @Override
    public boolean isEmpty() {
        if (start==null || end==null) return false;
        int cmp = start.compareTo(end);
        return cmp>0 || (cmp==0 && (!startInclusive || !endInclusive));
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(start).append(end).append(startInclusive).append(endInclusive).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        ProperInterval oth = (ProperInterval)other;
        if ((start==null ^ oth.start==null) || (end==null ^ oth.end==null)) return false;
        return start.equals(oth.start) && end.equals(oth.end) && endInclusive==oth.endInclusive && startInclusive==oth.startInclusive;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (startInclusive) b.append("[");
        else b.append("(");
        b.append(start).append(",").append(end);
        if (endInclusive) b.append("]");
        else b.append(")");
        return b.toString();
    }
}
