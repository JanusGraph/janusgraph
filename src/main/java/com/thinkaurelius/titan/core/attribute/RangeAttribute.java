package com.thinkaurelius.titan.core.attribute;


/**
 * Defines a class or data type that can be used for property types indexed as ranges.
 * 
 * To ensure that attributes of range-index property types are linearly ordered and properly serialized,
 * data types used with such property types must implement the RangeAttribute interface.
 * Note that user defined RangeAttributes must have a custom {@link com.thinkaurelius.titan.core.attribute.AttributeSerializer} registered with the graph
 * database.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * @see com.thinkaurelius.titan.core.PropertyIndex
 * @param <V> Type of specific RangeAttribute subclass
 */
public interface RangeAttribute<V> extends Comparable<V> {

}
