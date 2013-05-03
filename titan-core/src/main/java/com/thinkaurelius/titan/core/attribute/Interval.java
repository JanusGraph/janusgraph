package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Generic interval representation with a start and end point.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Interval<V extends Comparable<V>> {

    private final V start;
    private final V end;
    private final boolean startInclusive;
    private final boolean endInclusive;

    /**
     * Constructs a new range. The start point is considered inclusive and the end point exclusive.
     *
     * @param start Starting attribute of range
     * @param end   Ending attribute of range
     */
    public Interval(V start, V end) {
        this(start, end, true, false);
    }

    /**
     * Constructs a new range. The start point is considered inclusive and the end point exclusive.
     *
     * @param <V>   Type of Range
     * @param start Starting attribute of range
     * @param end   Ending attribute of range
     * @return Range for given end points
     */
    public static final <V extends Comparable<V>> Interval<V> of(V start, V end) {
        return new Interval<V>(start, end);
    }

    /**
     * Constructs a new range. The start point is considered inclusive and the end point inclusion is user defined.
     *
     * @param start        Starting attribute of range
     * @param end          Ending attribute of range
     * @param endInclusive Whether the end point is inclusive or not
     */
    private Interval(V start, V end, boolean endInclusive) {
        this(start, end, true, endInclusive);
    }

    /**
     * Constructs a new range where start and end point inclusion can be specified.
     *
     * @param start          Starting attribute of range
     * @param end            Ending attribute of range
     * @param startInclusive Whether the start point is inclusive or not
     * @param endInclusive   Whether the end point is inclusive or not
     */
    private Interval(V start, V end, boolean startInclusive, boolean endInclusive) {
        Preconditions.checkNotNull(start);
        Preconditions.checkNotNull(end);
        if (startInclusive && endInclusive)
            Preconditions.checkArgument(start.compareTo(end)<=0,"End must be greater or equal than start");
        else
            Preconditions.checkArgument(start.compareTo(end)<0,"End must be greater than start");

        this.start = start;
        this.end = end;
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
    }

    public V getEnd() {
        return end;
    }

    public V getStart() {
        return start;
    }

    public boolean endInclusive() {
        return endInclusive;
    }

    public boolean startInclusive() {
        return startInclusive;
    }

    public boolean inInterval(Object obj) {
        if (obj == null) return false;
        V p = (V) obj;
        int startcomp = p.compareTo(start);
        int endcomp = p.compareTo(end);
        if (startcomp < 0 || endcomp > 0) return false;
        else if (startcomp == 0 && !startInclusive) return false;
        else if (endcomp == 0 && !endInclusive) return false;
        else return true;
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
        Interval oth = (Interval)other;
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
