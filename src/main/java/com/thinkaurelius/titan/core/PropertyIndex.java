
package com.thinkaurelius.titan.core;

/**
 * Enumerates the four possible types of index structures that can be created to allow attribute based retrieval of nodes.
 * 
 * When a property type is defined, its index type can be set with the default being {@link #None} - i.e. no index is created.
 * When a property type has an index type defined, all properties of that type are indexed according to that type and this
 * index structure can be used to quickly retrieve the properties and nodes with a given attribute value.
 * 
 * <b>Note: currently only standard indexes are supported on keyed property types!</b>
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public enum PropertyIndex {

	/**
	 * No index structure is build for a property type with this index type
	 */
	None,
	
	/**
	 * A index structure for retrieving a single attribute for a given property type is build.
	 */
	Standard,
	
	/**
	 * An index structure for retrieving from ranges of attributes for a given property type is build.
	 * A property for which this index is defined must have a data type which is a sub-class of 
	 * {@link com.thinkaurelius.titan.core.attribute.RangeAttribute} to ensure that it is comparable
	 * (i.e. linearly ordered) and properly serialized.
	 */
	Range;
	
	/**
	 * Checks whether this index type represents a look-up index.
	 * All index types but {@link #None} return true.
	 * 
	 * @return true, if this index type has an index, else false.
	 */
	public boolean hasIndex() {
		switch(this) {
		case None: return false;
		case Standard: 
		case Range: return true;
		default: throw new AssertionError("Unexpected enum constant: " + this);
		}
	}
	
}
