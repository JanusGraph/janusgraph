package com.thinkaurelius.titan.graphdb.edgequery;

import com.google.common.base.Preconditions;

/**
 * A Range is a proper {@link Interval} with different start and end points.
 * 
 * Note that a range could also be defined with identical bounds. However, in that case it is preferable to use
 * {@link PointInterval} for efficiency.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * @param <V> Type of attribute for which the range is defined.
 */
class Range<V extends Comparable<V>> implements Interval<V> {

	private final V start;
	private final V end;
	private final boolean startInclusive;
	private final boolean endInclusive;
	
	/**
	 * Constructs a new range. The start point is considered inclusive and the end point exclusive.
	 * 
	 * @param start Starting attribute of range
	 * @param end Ending attribute of range
	 */
	public Range(V start, V end) {
		this(start,end,true,false);
	}
	
	/**
	 * Constructs a new range. The start point is considered inclusive and the end point exclusive.
	 * 
	 * @param <V> Type of Range
	 * @param start Starting attribute of range
	 * @param end Ending attribute of range
	 * @return Range for given end points
	 */
	public static final<V extends Comparable<V>> Range<V> of(V start, V end) {
		return new Range<V>(start,end);
	}
	
	/**
	 * Constructs a new range. The start point is considered inclusive and the end point inclusion is user defined.
	 * 
	 * @param start Starting attribute of range
	 * @param end Ending attribute of range
	 * @param endInclusive Whether the end point is inclusive or not
	 */
	public Range(V start, V end, boolean endInclusive) {
		this(start,end,true,endInclusive);
	}
	
	/**
	 * Constructs a new range where start and end point inclusion can be specified.
	 * 
	 * @param start Starting attribute of range
	 * @param end Ending attribute of range
	 * @param startInclusive Whether the start point is inclusive or not
	 * @param endInclusive Whether the end point is inclusive or not
	 */
	public Range(V start, V end, boolean startInclusive, boolean endInclusive) {
        Preconditions.checkNotNull(start);
        Preconditions.checkNotNull(end);
		this.start=start;
		this.end=end;
		this.startInclusive=startInclusive;
		this.endInclusive = endInclusive;
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

	@SuppressWarnings("unchecked")
	@Override
	public boolean inInterval(Object obj) {
		if (!(start.getClass().isAssignableFrom(obj.getClass()))) return false;
		V p = (V)obj;
		int startcomp = p.compareTo(start);
		int endcomp = p.compareTo(end);
		if (startcomp<0 || endcomp>0) return false;
		else if (startcomp==0 && !startInclusive) return false;
		else if (endcomp==0 && !endInclusive) return false;
		else return true;
	}

}
