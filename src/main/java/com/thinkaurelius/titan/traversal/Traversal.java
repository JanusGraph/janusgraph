package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.core.RelationshipType;

/**
 * Defines a graph traversal.
 * 
 * Traversing a graph starts at one or multiple seed nodes and proceeds from there along incident relationships until a termination condition is met.
 * The nodes and relationships visited during this traversal define a subgraph and can be retrieved for further manipulation or analysis.
 * 
 * Traversal is a means to extract subgraphs of interest from much larger graphs. The termination condition will ensure that only a (comparatively) small
 * subgraph is retrieved. However, if the underlying graph is small, the traversal may finish before the termination condition is met.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 *
 * @param <V> Type of the Traversal implementation
 */
public interface Traversal<V> {

	/**
	 * Adds a RelationshipType and associated direction to the list of RelationshipTypes to which this traversal is restricted.
	 * 
	 * If none is specified, the traversal will proceed along all incident relationships. Through this method, the traversal
	 * is restricted to only proceed along those relationships with matching type and direction.
	 * More than one relationship type and direction can be specified, but it is expected that multiple relationship type
	 * and direction pairs do not overlap, in the sense that they match the same relationships.
	 * 
	 * @param type Relationship type 
	 * @param dir Direction
	 * @return This traversal
	 */
	public V addRelationshipType(RelationshipType type, Direction dir);

	/**
	 * Adds a RelationshipType to the list of RelationshipTypes to which this traversal is restricted.
	 * 
	 * If none is specified, the traversal will proceed along all incident relationships. Through this method, the traversal
	 * is restricted to only proceed along those relationships with matching type and any direction.
	 * More than one relationship type can be specified, but it is expected that multiple relationship types
	 * do not overlap, in the sense that they match the same relationships.
	 * 
	 * @param type Relationship type 
	 * @return This traversal
	 */
	public V addRelationshipType(RelationshipType type);
	
	/**
	 * Adds a EdgeTypeGroup and associated direction to the list of EdgeTypeGroups to which this traversal is restricted.
	 * 
	 * If none is specified, the traversal will proceed along all incident relationships. Through this method, the traversal
	 * is restricted to only proceed along those relationships whose type is part of this edge type group.
	 * More than one edge type group and direction can be specified, but it is expected that multiple edge type group
	 * and direction pairs do not overlap, in the sense that they match the same relationships. For instance, adding an edge type
	 * group AND a relationship type which belongs to this group will be considered redundant and cause an exception to be
	 * thrown when traversing.
	 * 
	 * 
	 * @param group Edge type group
	 * @param dir Direction
	 * @return This traversal
	 */
	public V addEdgeTypeGroup(EdgeTypeGroup group, Direction dir);

	/**
	 * Adds a EdgeTypeGroup to the list of EdgeTypeGroups to which this traversal is restricted.
	 * 
	 * If none is specified, the traversal will proceed along all incident relationships. Through this method, the traversal
	 * is restricted to only proceed along those relationships whose type is part of this edge type group.
	 * More than one edge type group can be specified, but it is expected that multiple edge type groups
	 * do not overlap, in the sense that they match the same relationships. For instance, adding an edge type
	 * group AND a relationship type which belongs to this group will be considered redundant and cause an exception to be
	 * thrown when traversing.
	 * 
	 * 
	 * @param group Edge type group
	 * @return This traversal
	 */
	public V addEdgeTypeGroup(EdgeTypeGroup group);
	
	/**
	 * Terminates the traversal when the specified depth is reached.
	 * 
	 * Sets the termination condition to be met when the number of hops or steps of the traversal exceed the given depth.
	 * By default, the depth is set to the maximum integer value.
	 * 
	 * @param depth Maximum depth of the traversal.
	 * @return This traversal.
	 */
	public V setDepth(int depth);
	
	
	
}
