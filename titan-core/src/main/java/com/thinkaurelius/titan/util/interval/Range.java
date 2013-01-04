package com.thinkaurelius.titan.util.interval;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * A Range is a proper {@link AtomicInterval} with different start and end points.
 * <p/>
 * Note that a range could also be defined with identical bounds. However, in that case it is preferable to use
 * {@link PointInterval} for efficiency.
 * The start and/or end point of a range can be null which denotes the open interval in the start and/or end direction.
 *
 * @param <V> Type of attribute for which the range is defined.
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class Range<V extends Comparable<V>> implements AtomicInterval<V> {

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
    public Range(V start, V end) {
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
    public static final <V extends Comparable<V>> Range<V> of(V start, V end) {
        return new Range<V>(start, end);
    }

    /**
     * Constructs a new range. The start point is considered inclusive and the end point inclusion is user defined.
     *
     * @param start        Starting attribute of range
     * @param end          Ending attribute of range
     * @param endInclusive Whether the end point is inclusive or not
     */
    public Range(V start, V end, boolean endInclusive) {
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
    public Range(V start, V end, boolean startInclusive, boolean endInclusive) {
        this.start = start;
        this.end = end;
        this.startInclusive = start == null ? true : startInclusive;
        this.endInclusive = end == null ? true : endInclusive;
    }

    @Override
    public V getEndPoint() {
        return end;
    }

    @Override
    public V getStartPoint() {
        return start;
    }

    @Override
    public boolean isPoint() {
        return false;
    }

    @Override
    public boolean isRange() {
        return true;
    }

    @Override
    public boolean endInclusive() {
        return endInclusive;
    }

    @Override
    public boolean startInclusive() {
        return startInclusive;
    }

    @Override
    public boolean hasHoles() {
        return false;
    }

    @Override
    public Set<V> getHoles() {
        return ImmutableSet.of();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean inInterval(Object obj) {
        if (obj == null) return false;
        if (start == null && end == null) return true;
        V p = (V) obj;
        int startcomp = start == null ? 1 : p.compareTo(start);
        int endcomp = end == null ? -1 : p.compareTo(end);
        if (startcomp < 0 || endcomp > 0) return false;
        else if (startcomp == 0 && !startInclusive) return false;
        else if (endcomp == 0 && !endInclusive) return false;
        else return true;
    }

    @Override
    public AtomicInterval<V> intersect(AtomicInterval<V> other) {
        if (other.isPoint() || other.hasHoles()) {
            return other.intersect(this);
        } else {
            assert other.isRange();
            Range<V> o = (Range<V>) other;
            V nstart, nend;
            boolean sInc, eInc;
            if (start == null) {
                nstart = o.start;
                sInc = o.startInclusive;
            } else if (o.start == null) {
                nstart = start;
                sInc = startInclusive;
            } else {
                int comp = start.compareTo(o.start);
                if (comp < 0) {
                    nstart = o.start;
                    sInc = o.startInclusive;
                } else if (comp == 0) {
                    nstart = start;
                    sInc = startInclusive && o.startInclusive;
                } else { //comp>0
                    nstart = start;
                    sInc = startInclusive;
                }
            }

            if (end == null) {
                nend = o.end;
                eInc = o.endInclusive;
            } else if (o.end == null) {
                nend = end;
                eInc = endInclusive;
            } else {
                int comp = end.compareTo(o.end);
                if (comp < 0) {
                    nend = end;
                    eInc = endInclusive;
                } else if (comp == 0) {
                    nend = end;
                    eInc = endInclusive && o.endInclusive;
                } else { //comp>0
                    nend = o.end;
                    eInc = o.endInclusive;
                }
            }
            return new Range(nstart, nend, sInc, eInc);
        }
    }


}
