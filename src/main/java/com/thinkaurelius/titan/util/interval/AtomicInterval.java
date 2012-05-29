package com.thinkaurelius.titan.util.interval;


import java.util.Set;

/**
 * Defines and interval of attribute values.
 * 
 * Intervals are needed when querying for vertices which have attributes in a range of values.
 * Intervals have start and end points which are either inclusive or exclusive. An interval can be a proper {@link Range},
 * where the start point is different from the end point, or a {@link PointInterval} where those points are identical.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * @param <V> Type of attribute for which the interval is defined.
 */
public interface AtomicInterval<V> {

	/**
	 * Returns the start point of the interval
	 * 
	 * @return Tail point of the interval
	 */
	public V getStartPoint();
	
	/**
	 * Returns the end point of the interval
	 * 
	 * @return Head point of the interval
	 */
	public V getEndPoint();
	
	/**
	 * Checks whether this interval is a point-interval
	 * 
	 * @return True, if this interval is a point-interval, else false.
	 * @see PointInterval
	 */
	public boolean isPoint();
	
	/**
	 * Checks whether this interval is a range or proper interval.
	 * 
	 * @return True, if this interval is a range, else false.
	 * @see Range
	 */
	public boolean isRange();
	
	/**
	 * Checks whether the end point of this interval is inclusive.
	 * 
	 * The end point is inclusive if it is part of the interval, else it is
	 * considered exclusive.
	 * 
	 * @return True, if the end point of the interval is inclusive, else false.
	 */
	public boolean endInclusive();
	
	/**
	 * Checks whether the start point of this interval is inclusive.
	 * 
	 * The start point is inclusive if it is part of the interval, else it is
	 * considered exclusive.
	 * 
	 * @return True, if the start point of the interval is inclusive, else false.
	 */
	public boolean startInclusive();

    /**
     * Checks whether this interval has any holes, i.e. point values that are excluded from the interval.
     *
     * @return True, if the interval has holes, else false.
     */
    public boolean hasHoles();

    /**
     * Returns the set of holes, i.e. excluded points, for this interval or an empty-set if non exist.
     *
     * @return The set of holes, i.e. excluded points, for this interval or an empty-set if non exist.
     */
    public Set<V> getHoles();
	
	/**
	 * Checks whether the given object is in the interval.
	 * 
	 * Note that an object with type incompatible with type parameter V will never
	 * be in this interval.
	 * 
	 * @param obj Object to check interval membership for.
	 * @return True, if object is in interval, else false.
	 */
	public boolean inInterval(Object obj);

    /**
     * Intersects the current interval with the given interval in the mathematical sense and returns the result.
     * @param other Other interval
     * @return Intersection between intervals or null if the intersection is empty
     */
    public AtomicInterval<V> intersect(AtomicInterval<V> other);
	
}
