
package com.thinkaurelius.titan.core;

/**
 * The EdgeTypeMaker is a constructor for {@link com.thinkaurelius.titan.core.EdgeType}s.
 * Using the edge type maker's methods, the following characteristics of an edge type can be defined:
 * <ul>
 * <li>Name of the edge type. No default value</li>
 * <li>Whether the edge type is functional. Default value: false</li>
 * <li>Directionality of the edge type. Default value: {@link com.thinkaurelius.titan.core.Directionality#Directed} </li>
 * <li>Category of the edge type. Default value: {@link com.thinkaurelius.titan.core.EdgeCategory#Labeled}</li>
 * <li>Group of the edge type. Default value: {@link com.thinkaurelius.titan.core.EdgeTypeGroup#DefaultGroup}</li>
 * <li>Whether or not the property type is keyed. Default value: false</li>
 * <li>The data type of the property type. No default value</li>
 * <li>Whether properties of the type should be indexed. Default is: no hasIndex</li>
 * <li>The key and compact signature of the edge type. Default value: no signature</li>
 * </ul>
 * 
 * Once the user has set all characteristics, a relationship type or property type can be created using the 
 * methods {@link #makeRelationshipType()} and {@link #makePropertyType()} respectively.
 * When there is no default value, the user has to set it explicitly.
 * 
 * @see	com.thinkaurelius.titan.core.EdgeType
 * @see PropertyType
 * @see RelationshipType
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface EdgeTypeMaker {

	/**
	 * Sets the name of the edge type to be created
	 * 
	 * @param name Name of the edge type
	 * @return This edge type maker
	 */
	public EdgeTypeMaker withName(String name);
	
	/**
	 * Sets whether or not this edge type is functional.
	 * 
	 * @param isfunctional Value to be set
	 * @return This edge type maker
	 */
	public EdgeTypeMaker functional(boolean isfunctional);

	/**
	 * Sets the directionality of the edge type to be created
	 * @param dir Directionality to be set
	 * @return This edge type maker
	 * @see com.thinkaurelius.titan.core.Directionality
	 */
	public EdgeTypeMaker withDirectionality(Directionality dir);
	
	/**
	 * Sets the category of the edge type to be created
	 * 
	 * @param cat Category to be set
	 * @return  This edge type maker
	 * @see com.thinkaurelius.titan.core.EdgeCategory
	 */
	public EdgeTypeMaker category(EdgeCategory cat);
	
	/**
	 * Sets the group of the edge type to be created
	 * 
	 * @param group Group to be set
	 * @return  This edge type maker
	 * @see com.thinkaurelius.titan.core.EdgeTypeGroup
	 */
	public EdgeTypeMaker group(EdgeTypeGroup group);
	
	/**
	 * Adds the specified edge type to the key-signature of the edge type to be created.
	 * 
	 * Specifying the signature of a labeled or label-restricted edge type facilitates a more compact
	 * representation of edges and has a positive impact on the database performance.
	 * A signature edge type must be functional and a property type or a unidirected relationship type.
	 * 
	 * In contrast to the compact signature, the key-signature can be used to select a subset of incident
	 * edges when querying edges via {@link com.thinkaurelius.titan.core.EdgeQuery}.
	 * 
	 * @param et Edge type to add to the key signature
	 * @return This edge type maker
	 */
	public EdgeTypeMaker keySignature(EdgeType... et);
	
	/**
	 * Adds the specified edge type to the compact-signature of the edge type to be created.
	 * 
	 * Specifying the signature of a labeled or label-restricted edge type facilitates a more compact
	 * representation of edges and has a positive impact on the database performance.
	 * A signature edge type must be functional and a property type or a unidirected relationship type.
	 * 
	 * 
	 * @param et Edge type to add to the key signature
	 * @return This edge type maker
	 */
	public EdgeTypeMaker compactSignature(EdgeType... et);
	
	/**
	 * Sets the edge type to be created as keyed
	 * 
	 * Note that this is only relevant when creating property types and will
	 * be ignored for relationship types.
	 * 
	 * @return  This edge type maker
	 * @see PropertyType#isKeyed()
	 */
	public EdgeTypeMaker makeKeyed();
	
	/**
	 * Sets whther the property type to be created is indexed.
	 * 
	 * Note that this is only relevant when creating property types and will
	 * be ignored for relationship types.
	 * 
	 * @param index true if hasIndex should be created, else false
	 * @return  This edge type maker
	 */
	public EdgeTypeMaker withIndex(boolean index);
	
//	public EdgeTypeMaker textIndex();
	
//	public EdgeTypeMaker addDedicatedIndex(DedicatedIndex hasIndex);
	
	/**
	 * Sets the data type of the property type to be created
	 * 
	 * Note that this is only relevant when creating property types and will
	 * be ignored for relationship types.
	 * 
	 * @param clazz Data type to be set
	 * @return  This edge type maker
	 * @see PropertyType#getDataType()
	 */
	public EdgeTypeMaker dataType(Class<?> clazz);
	
	/**
	 * Creates a new relationship type according to the configured characteristics.
	 * 
	 * @return A new relationship type
	 * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
	 */
	public RelationshipType makeRelationshipType();
	
	/**
	 * Creates a new property type according to the configured characteristics.
	 * 
	 * @return A new property type
	 * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
	 */
	public PropertyType makePropertyType();
	
}
