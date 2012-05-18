
package com.thinkaurelius.titan.core;

/**
 * The TypeMaker is a constructor for {@link TitanType}s.
 * Using the edge type maker's methods, the following characteristics of an edge type can be defined:
 * <ul>
 * <li>Name of the edge type. No default value</li>
 * <li>Whether the edge type is functional. Default value: false</li>
 * <li>Directionality of the edge type. Default value: {@link com.thinkaurelius.titan.graphdb.edgetypes.Directionality#Directed} </li>
 * <li>Category of the edge type. Default value: {@link com.thinkaurelius.titan.graphdb.edgetypes.EdgeCategory#HasProperties}</li>
 * <li>Group of the edge type. Default value: {@link TypeGroup#DEFAULT_GROUP}</li>
 * <li>Whether or not the property type is keyed. Default value: false</li>
 * <li>The data type of the property type. No default value</li>
 * <li>Whether properties of the type should be indexed. Default is: no hasIndex</li>
 * <li>The key and compact signature of the edge type. Default value: no signature</li>
 * </ul>
 * 
 * Once the user has set all characteristics, a relationship type or property type can be created using the 
 * methods {@link #makeEdgeLabel} and {@link #makePropertyKey} respectively.
 * When there is no default value, the user has to set it explicitly.
 * 
 * @see    TitanType
 * @see TitanKey
 * @see TitanLabel
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TypeMaker {

	/**
	 * Sets the name of the edge type to be created
	 * 
	 * @param name Name of the edge type
	 * @return This edge type maker
	 */
	public TypeMaker name(String name);
	
	/**
	 * Sets whether or not this edge type is functional.
	 * 
	 * @return This edge type maker
	 */
	public TypeMaker functional();

	public TypeMaker directed();
    
    public TypeMaker undirected();
    
    public TypeMaker unidirected();
	
	public TypeMaker simple();
    

	/**
	 * Sets the group of the edge type to be created
	 * 
	 * @param group Group to be set
	 * @return  This edge type maker
	 * @see TypeGroup
	 */
	public TypeMaker group(TypeGroup group);
	
	/**
	 * Adds the specified edge type to the key-signature of the edge type to be created.
	 * 
	 * Specifying the signature of a labeled or label-restricted edge type facilitates a more compact
	 * representation of edges and has a positive impact on the database performance.
	 * A signature edge type must be functional and a property type or a unidirected relationship type.
	 * 
	 * In contrast to the compact signature, the key-signature can be used to select a subset of incident
	 * edges when querying edges via {@link TitanQuery}.
	 * 
	 * @param et TitanRelation type to add to the key signature
	 * @return This edge type maker
	 */
	public TypeMaker primaryKey(TitanType... et);
	
	/**
	 * Adds the specified edge type to the compact-signature of the edge type to be created.
	 * 
	 * Specifying the signature of a labeled or label-restricted edge type facilitates a more compact
	 * representation of edges and has a positive impact on the database performance.
	 * A signature edge type must be functional and a property type or a unidirected relationship type.
	 * 
	 * 
	 * @param et TitanRelation type to add to the key signature
	 * @return This edge type maker
	 */
	public TypeMaker signature(TitanType... et);
	
	/**
	 * Sets the edge type to be created as keyed
	 * 
	 * Note that this is only relevant when creating property keys and will
	 * be ignored for relationship types.
	 * 
	 * @return  This edge type maker
	 * @see TitanKey#isUnique()
	 */
	public TypeMaker unique();
	
	/**
	 * Sets whther the property type to be created is indexed.
	 * 
	 * Note that this is only relevant when creating property types and will
	 * be ignored for relationship types.
	 * 
	 * @param index true if hasIndex should be created, else false
	 * @return  This edge type maker
	 */
	public TypeMaker indexed();
	
	/**
	 * Sets the data type of the property type to be created
	 * 
	 * Note that this is only relevant when creating property types and will
	 * be ignored for relationship types.
	 * 
	 * @param clazz Data type to be set
	 * @return  This edge type maker
	 * @see TitanKey#getDataType()
	 */
	public TypeMaker dataType(Class<?> clazz);
	
	/**
	 * Creates a new relationship type according to the configured characteristics.
	 * 
	 * @return A new relationship type
	 * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
	 */
	public TitanLabel makeEdgeLabel();
	
	/**
	 * Creates a new property type according to the configured characteristics.
	 * 
	 * @return A new property type
	 * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
	 */
	public TitanKey makePropertyKey();
	
}
