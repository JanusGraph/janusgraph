
package com.thinkaurelius.titan.core;


/**
 * TitanKey is an extension of {@link TitanType} for properties.
 *
 * In addition to {@link TitanType}, TitanKey defines:
 * <ul>
 *     <li><strong>Data Type:</strong> The accepted types of attribute values.</li>
 *     <li><strong>Index:</strong> Whether attribute values are indexed. If a property key is configured to be indexed,
 *     then all properties with that key are indexed which means one can retrieve vertices for that key and a value
 *     via {@link TitanTransaction#getVertices(TitanKey, Object)} </li>
 *     <li><strong>Uniqueness:</strong> When a TitanKey is configured to be unique it is ensured that at most one
 *     vertex can be associated with a particular value for that key. As an example, <i>social security number</i>
 *     is a unique property, since each SSN is associated with only one individual.</li>
 * </ul>
 *
 * @see TitanType
 *
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com) 
 *
 */
public interface TitanKey extends TitanType {
	
	/**
	 * Returns the data type for this property key.
	 * The attributes of all properties of this type must be an instance of this data type.
	 * 
	 * @return Data type for this property key.
	 */
	public Class<?> getDataType();
	
	/**
	 * Returns true if properties of this key are indexed.
	 * 
	 * @return true if properties of this key are indexed, else false
	 */
	public boolean hasIndex();
	
	/**
	 * Checks whether this property key is unique.
	 * A property key is <b>unique</b> if all attributes for properties of this key are uniquely associated with the
	 * property's vertex. In other words, there is a functional mapping from attribute values to vertices.
	 * 
	 * @return true, if this property key is unique, else false.
	 */
	public boolean isUnique();
	
}
