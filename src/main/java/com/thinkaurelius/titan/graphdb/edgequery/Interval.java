package com.thinkaurelius.titan.graphdb.edgequery;


/**
 * Defines and interval of attribute values.
 * 
 * Intervals are needed when querying for nodes which have attributes in a range of values.
 * Intervals have start and end points which are either inclusive or exclusive. An interval can be a proper {@link Range},
 * where the start point is different from the end point, or a {@link PointInterval} where those points are identical.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * @param <V> Type of attribute for which the interval is defined.
 */
public interface Interval<V> {

	/**
	 * Returns the start point of the interval
	 * 
	 * @return Start point of the interval
	 */
	public V getStartPoint();
	
	/**
	 * Returns the end point of the interval
	 * 
	 * @return End point of the interval
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
	 * Checks whether the given object is in the interval.
	 * 
	 * Note that an object with type incompatible with type parameter V will never
	 * be in this interval.
	 * 
	 * @param obj Object to check interval membership for.
	 * @return True, if object is in interval, else false.
	 */
	public boolean inInterval(Object obj);
	
}
