
package com.thinkaurelius.titan.core;

import java.util.Iterator;



/**
 * {@link com.thinkaurelius.titan.core.Node} is one of the two basic entities of a graph - the other one being {@link com.thinkaurelius.titan.core.Edge}.
 * A node, also called vertex, can represent an object, individual, or other identifiable entity which have properties ({@link Property}
 * and engages in relationships ({@link Relationship}). Nodes are connected to one other via relationships. 
 * For more information on the mathematical definition of a graph and the concept of nodes see 
 * <a href="http://en.wikipedia.org/wiki/Graph_%28mathematics%29">Graph Definition</a>.
 * 
 * Nodes are the first thing to be created in a graph in the context of a {@link com.thinkaurelius.titan.core.GraphTransaction}. Nodes have incident {@link com.thinkaurelius.titan.core.Edge}s which
 * are either {@link Relationship}s to other nodes or {@link Property}es defining attributes of the node such as unique ids.
 * 
 * @see	com.thinkaurelius.titan.core.Edge
 * @see Relationship
 * @see Property
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface Node extends ReferenceNode {

	
	/* ---------------------------------------------------------------
	 * Incident Edge Access methods
	 * ---------------------------------------------------------------
	 */
	
	/**
	 * Creates a new relationship incident on this node.
	 * 
	 * Creates and returns a new {@link Relationship} of the specified type with this node being the start
	 * node and the given node being the end node. 
	 * Hence, the created relationship is binary. Hyperedges can only be created through {@link com.thinkaurelius.titan.core.GraphTransaction}s.
	 * 
	 * @param relType	type of the relationship to be created
	 * @param node		end point of the relationship to be created
	 * @return 			New relationship of specified type
	 */
	public Relationship createRelationship(RelationshipType relType, Node node);
	
	/**
	 * Creates a new relationship incident on this node.
	 * 
	 * Creates and returns a new {@link Relationship} of the specified type with this node being the start
	 * node and the given node being the end node. 
	 * Hence, the created relationship is binary. Hyperedges can only be created through {@link com.thinkaurelius.titan.core.GraphTransaction}s.
	 * 
	 * @param relType	name of the relationship to be created
	 * @param node		end point of the relationship to be created
	 * @return 			New relationship of specified type
	 * @throws	IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type with said name has not yet been created.
	 */
	public Relationship createRelationship(String relType, Node node);
		
	/**
	 * Creates a new property for this node with the specified attribute
	 * 
	 * Creates and returns a new {@link Property} of the specified type with this node being the start
	 * node and the given object being the attribute.
	 * 
	 * @param propType	type of the property to be created
	 * @param attribute	attribute of the property to be created
	 * @return 			New property of specified type
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property type.
	 */
	public Property createProperty(PropertyType propType, Object attribute);
	
	/**
	 * Creates a new property for this node with the specified attribute
	 * 
	 * Creates and returns a new {@link Property} of the specified type with this node being the start
	 * node and the given object being the attribute.
	 * 
	 * @param propType	name of the property to be created
	 * @param attribute	attribute of the property to be created
	 * @return 			New property of specified type
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property type.
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public Property createProperty(String propType, Object attribute);

	
	/* ---------------------------------------------------------------
	 * Incident Edge Access methods
	 * ---------------------------------------------------------------
	 */
	
	/**
	 * Starts a new EdgeQuery for this node.
	 * 
	 * Initializes and returns a new {@link com.thinkaurelius.titan.core.EdgeQuery} centered on this node.
	 * 
	 * @return New EdgeQuery for this node
	 * @see com.thinkaurelius.titan.core.EdgeQuery
	 */
	public EdgeQuery edgeQuery();

	/**
	 * Retrieves the attribute value for the only property of the specified property type incident on this node.
	 * 
	 * This method call expects that there is at most one property of the specified {@link PropertyType} incident on this node.
	 * 
	 * @param type Property type of the property for which to retrieve the attribute value 
	 * @return Attribute value of the property with the specified type or null if no such property exists
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 */
	public Object getAttribute(PropertyType type);
	
	/**
	 * Retrieves the attribute value for the only property of the specified property type incident on this node.
	 * 
	 * This method call expects that there is at most one property of the specified {@link PropertyType} incident on this node.
	 * 
	 * @param type Property type name of the property for which to retrieve the attribute value 
	 * @return Attribute value of the property with the specified type or null if no such property exists
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public Object getAttribute(String type);

	/**
	 * Retrieves the attribute value for the only property of the specified property type incident on this node and casts
	 * it to the specified {@link Class}.
	 * 
	 * This method call expects that there is at most one property of the specified {@link PropertyType} incident on this node.
	 * 
	 * @param type Property type name of the property for which to retrieve the attribute value 
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 */
	public<O> O getAttribute(PropertyType type, Class<O> clazz);
	
	/**
	 * Retrieves the attribute value for the only property of the specified property type incident on this node and casts
	 * it to the specified {@link Class}.
	 * 
	 * This method call expects that there is at most one property of the specified {@link PropertyType} incident on this node.
	 * 
	 * @param type Property type of the property for which to retrieve the attribute value 
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public<O> O getAttribute(String type, Class<O> clazz);

	/**
	 * Retrieves the String attribute for the only property of the specified property type incident on this node.
	 * 
	 * This method call expects that there is at most one property of the specified {@link PropertyType} incident on this node.
	 * The retrieved attribute is cast to a String.
	 * 
	 * @param type Property type of the property for which to retrieve the attribute value 
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 */
	public String getString(PropertyType type);
	
	/**
	 * Retrieves the String attribute for the only property of the specified property type incident on this node.
	 * 
	 * This method call expects that there is at most one property of the specified {@link PropertyType} incident on this node.
	 * The retrieved attribute is cast to a String.
	 * 
	 * @param type Property type name of the property for which to retrieve the attribute value 
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public String getString(String type);

	/**
	 * Retrieves the Number attribute for the only property of the specified property type incident on this node.
	 * 
	 * This method call expects that there is at most one property of the specified {@link PropertyType} incident on this node.
	 * The retrieved attribute is cast to a Number.
	 * 
	 * @param type Property type of the property for which to retrieve the attribute value 
	 * @return Attribute value of the property with the specified type
 	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 */
	public Number getNumber(PropertyType type);

	/**
	 * Retrieves the Number attribute for the only property of the specified property type incident on this node.
	 * 
	 * This method call expects that there is at most one property of the specified {@link PropertyType} incident on this node.
	 * The retrieved attribute is cast to a Number.
	 * 
	 * @param type Property type of the property for which to retrieve the attribute value 
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public Number getNumber(String type);

	

	/***
	 * Returns an iterable over all relationships of the specified relationship type in the given direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type and the direction of the relationship from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param relType RelationType of the returned relationships
	 * @param d Direction of the returned relationships with respect to this node
	 * @return Iterable over all relationships of the specified type in the given direction incident on this node
	 */
	public Iterable<Relationship> getRelationships(RelationshipType relType, Direction d);

	/***
	 * Returns an iterable over all relationships of the specified relationship type in the given direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type and the direction of the relationship from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param relType RelationType name of the returned relationships
	 * @param d Direction of the returned relationships with respect to this node
	 * @return Iterable over all relationships of the specified type in the given direction incident on this node
	 * @throws IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type 
	 * with said name has not yet been created.
	 */
	public Iterable<Relationship> getRelationships(String relType, Direction d);
	
	/***
	 * Returns an iterable over all relationships with their relationship type belonging to the specified {@link com.thinkaurelius.titan.core.EdgeTypeGroup} in the
	 * given direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships have 
	 * a relationship type belonging to the specified group and the direction of the relationship from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param group EdgeTypeGroup of the returned relationships
	 * @param d Direction of the returned relationships with respect to this node
	 * @return Iterable over all relationships of the specified group in the given direction incident on this node
	 */
	public Iterable<Relationship> getRelationships(EdgeTypeGroup group, Direction d);
	
	/***
	 * Returns an iterable over all relationships with their relationship type belonging to the specified {@link com.thinkaurelius.titan.core.EdgeTypeGroup} incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships have 
	 * a relationship type belonging to the specified group.
	 * 
	 * @param group EdgeTypeGroup of the returned relationships
	 * @return Iterable over all relationships of the specified group incident on this node
	 */
	public Iterable<Relationship> getRelationships(EdgeTypeGroup group);

	/***
	 * Returns an iterable over all relationships incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All incident relationships
	 * are returned irrespective their type or direction.
	 * 
	 * @return Iterable over all relationships incident on this node
	 */
	public Iterable<Relationship> getRelationships();
	
	
	/***
	 * Returns an iterable over all relationships in the specified direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. The direction of the 
	 * returned relationships from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param dir Direction of the returned relationships with respect to this node
	 * @return Iterable over all relationships in the given direction incident on this node
	 */
	public Iterable<Relationship> getRelationships(Direction dir);
	
	/***
	 * Returns an iterable over all relationships of the specified relationship type on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type.
	 * 
	 * @param relType RelationType of the returned relationships
	 * @return Iterable over all relationships of the specified type incident on this node
	 */
	public Iterable<Relationship> getRelationships(RelationshipType relType);

	/***
	 * Returns an iterable over all relationships of the specified relationship type on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type.
	 * 
	 * @param relType RelationType name of the returned relationships
	 * @return Iterable over all relationships of the specified type incident on this node
	 * @throws IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type 
	 * with said name has not yet been created.
	 */
	public Iterable<Relationship> getRelationships(String relType);

	
	/***
	 * Returns an iterator over all relationships of the specified relationship type in the given direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type and the direction of the relationship from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param relType RelationType of the returned relationships
	 * @param d Direction of the returned relationships with respect to this node
	 * @return Iterator over all relationships of the specified type in the given direction incident on this node
	 */
	public Iterator<Relationship> getRelationshipIterator(RelationshipType relType, Direction d);

	/***
	 * Returns an iterator over all relationships of the specified relationship type in the given direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type and the direction of the relationship from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param relType RelationType name of the returned relationships
	 * @param d Direction of the returned relationships with respect to this node
	 * @return Iterator over all relationships of the specified type in the given direction incident on this node
	 * @throws IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type 
	 * with said name has not yet been created.
	 */
	public Iterator<Relationship> getRelationshipIterator(String relType, Direction d);
	

	/***
	 * Returns an iterator over all relationships incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All incident relationships
	 * are returned irrespective their type or direction.
	 * 
	 * @return Iterator over all relationships incident on this node
	 */
	public Iterator<Relationship> getRelationshipIterator();
	
	
	/***
	 * Returns an Iterator over all relationships in the specified direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. The direction of the 
	 * returned relationships from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param dir Direction of the returned relationships with respect to this node
	 * @return Iterator over all relationships in the given direction incident on this node
	 */
	public Iterator<Relationship> getRelationshipIterator(Direction dir);
	
	/***
	 * Returns an Iterator over all relationships of the specified relationship type incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type.
	 * 
	 * @param relType RelationType of the returned relationships
	 * @return Iterator over all relationships of the specified type incident on this node
	 */
	public Iterator<Relationship> getRelationshipIterator(RelationshipType relType);

	/***
	 * Returns an Iterator over all relationships of the specified relationship type incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type.
	 * 
	 * @param relType RelationType name of the returned relationships
	 * @return Iterator over all relationships of the specified type incident on this node
	 * @throws IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type 
	 * with said name has not yet been created.
	 */
	public Iterator<Relationship> getRelationshipIterator(String relType);


	/***
	 * Returns an iterable over all properties incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the properties are returned. All properties incident
	 * on this node are returned irrespective of type.
	 * 
	 * @return Iterable over all properties incident on this node
	 */
	public Iterable<Property> getProperties();
		
	/***
	 * Returns an iterable over all properties of the specified property type incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the properties are returned. All returned properties are 
	 * of the specified type.
	 * 
	 * @param propType PropertyType of the returned properties
	 * @return Iterable over all properties of the specified type incident on this node
	 */
	public Iterable<Property> getProperties(PropertyType propType);
	
	/***
	 * Returns an iterable over all properties of the specified property type incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the properties are returned. All returned properties are 
	 * of the specified type.
	 * 
	 * @param propType PropertyType name of the returned properties
	 * @return Iterable over all properties of the specified type incident on this node
	 * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public Iterable<Property> getProperties(String propType);

	/***
	 * Returns an iterable over all properties with their property type belonging to the specified {@link com.thinkaurelius.titan.core.EdgeTypeGroup} in the
	 * given direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the properties are returned. All returned properties have 
	 * a property type belonging to the specified group and the direction of the property from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param group EdgeTypeGroup of the returned properties
	 * @param d Direction of the returned properties with respect to this node
	 * @return Iterable over all properties of the specified group in the given direction incident on this node
	 */
	public Iterable<Property> getProperties(EdgeTypeGroup group, Direction d);
	
	/***
	 * Returns an iterable over all properties with their property type belonging to the specified {@link com.thinkaurelius.titan.core.EdgeTypeGroup} incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the properties are returned. All returned properties have 
	 * a property type belonging to the specified group.
	 * 
	 * @param group EdgeTypeGroup of the returned properties
	 * @return Iterable over all properties of the specified group incident on this node
	 */
	public Iterable<Property> getProperties(EdgeTypeGroup group);
	
	/***
	 * Returns an Iterator over all properties incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the properties are returned. All properties incident
	 * on this node are returned irrespective of type.
	 * 
	 * @return Iterator over all properties incident on this node
	 */
	public Iterator<Property> getPropertyIterator();
	
	/***
	 * Returns an Iterator over all properties of the specified property type incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the properties are returned. All returned properties are 
	 * of the specified type.
	 * 
	 * @param propType PropertyType of the returned properties
	 * @return Iterator over all properties of the specified type incident on this node
	 */
	public Iterator<Property> getPropertyIterator(PropertyType propType);
	
	/***
	 * Returns an Iterator over all properties of the specified property type incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the properties are returned. All returned properties are 
	 * of the specified type.
	 * 
	 * @param propType PropertyType name of the returned properties
	 * @return Iterator over all properties of the specified type incident on this node
	 * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public Iterator<Property> getPropertyIterator(String propType);

	
	/***
	 * Returns an iterable over all edges incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the edges are returned. All edges incident
	 * on this node, meaning both relationships and properties, are returned irrespective of type and direction.
	 * 
	 * @return Iterable over all properties of the specified type incident on this node
	 */
	public Iterable<Edge> getEdges();
	
	/***
	 * Returns an iterable over all edges incident on this node in the specified direction.
	 * 
	 * There is no guarantee concerning the order in which the edges are returned. All edges
	 * which have the specified direction from the perspective of this node are returned.
	 * Note that both relationships and properties are returned.
	 * 
	 * @param dir Direction of the edges returned
	 * @return Iterable over all edges in the given direction incident on this node
	 */
	public Iterable<Edge> getEdges(Direction dir);
	
	
	/***
	 * Returns an iterator over all edges incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the edges are returned. All edges incident
	 * on this node, meaning both relationships and properties, are returned irrespective of type and direction.
	 * 
	 * @return Iterator over all properties of the specified type incident on this node
	 */
	public Iterator<Edge> getEdgeIterator();
	
	/***
	 * Returns an Iterator over all edges incident on this node in the specified direction.
	 * 
	 * There is no guarantee concerning the order in which the edges are returned. All edges
	 * which have the specified direction from the perspective of this node are returned.
	 * Note that both relationships and properties are returned.
	 * 
	 * @param dir Direction of the edges returned
	 * @return Iterator over all edges in the given direction incident on this node
	 */
	public Iterator<Edge> getEdgeIterator(Direction dir);

	
	


	/***
	 * Returns the number of edges incident on this node.
	 * 
	 * The number of edges incident on this node is the sum of the number of relationships and the number of properties
	 * incident on this node.
	 * 
	 * @return The total number of edges incident on this node
	 * @see #getNoProperties()
	 * @see #getNoRelationships()
	 */
	public int getNoEdges();
	
	/***
	 * Returns the number of relationships incident on this node.
	 * 
	 * Returns the total number of relationships irrespective of type and direction.
	 * Note, that self-loop relationships, i.e. relationships with identical start and end node, might
	 * get counted twice depending on implementation.
	 * 
	 * @return The number of relationships incident on this node.
	 */
	public int getNoRelationships();
	
	/***
	 * Returns the number of properties incident on this node.
	 * 
	 * Returns the total number of properties irrespective of type and direction.
	 * 
	 * @return The number of properties incident on this node.
	 */
	public int getNoProperties();
	
	/**
	 * Checks whether this node has at least one incident relationship.
	 * In other words, it returns getNoRelationships()>0, but might be implemented more efficiently.
	 * 
	 * @return true, if this node has at least one incident relationship, else false
	 */
	public boolean isConnected();
	
}
