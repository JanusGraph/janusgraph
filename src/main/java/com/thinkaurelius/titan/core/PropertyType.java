
package com.thinkaurelius.titan.core;


/**
 * Property type defines the schema for properties. 
 * In addition to {@link com.thinkaurelius.titan.core.EdgeType}, a property type defines the data type (i.e. class) of the attributes and specifies
 * index structures to use for efficient retrieval of properties by their attributes (if any).
 * 
 * @see	com.thinkaurelius.titan.core.EdgeType
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface PropertyType extends EdgeType {
	
	/**
	 * Returns the data type for this property type.
	 * The attributes of all properties of this type must be an instance of this data type.
	 * 
	 * @return Data type for this property type.
	 */
	public Class<?> getDataType();
	
	/**
	 * Returns the index type for this property type.
	 * 
	 * @return Index type for this property type.
	 * @see com.thinkaurelius.titan.core.PropertyIndex
	 */
	public PropertyIndex getIndexType();
	
	/**
	 * Checks whether this property type is keyed.
	 * A property type is <b>keyed</b> if all attributes for properties of this type are uniquely associated with the
	 * properties start node. In other words, there is a functional mapping from attribute values to start nodes.
	 * 
	 * @return true, if this property type is keyed, else false.
	 */
	public boolean isKeyed();
	
}
