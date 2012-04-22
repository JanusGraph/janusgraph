
package com.thinkaurelius.titan.core;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.edgetypes.group.StandardEdgeTypeGroup;

/**
 * Grouping of {@link com.thinkaurelius.titan.core.EdgeType}. To facilitate efficient joint retrieval of edges for multiple edge types, those
 * edge types can be assigned to an edge type group.
 * The edge type group of an edge type is defined when the edge type is first created.
 * This abstract class acts as a factory method to create edge type groups which are defined by an id and
 * an associated name. Edges are grouped by their edge type's group id, so different edge type groups with
 * equivalent id are also treated as equivalent for retrieval purposes.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public abstract class EdgeTypeGroup {
	
    private static final int MAX_GROUP_ID = (2<<6)-1;
    
	/**
	 * The default edge type group when no group is specified during edge type construction.
     * Note: system edge types have group with id 0
	 * @see EdgeTypeMaker
	 */
	public final static EdgeTypeGroup DefaultGroup = of((short)1,"Default Group");
	
	protected EdgeTypeGroup() {};
	
	/**
	 * Creates and returns a new edge type group with the specified id and name
	 * @param id ID of the edge type group
	 * @param name Name of the edge type group
	 * @return An edge type group
	 * @throws IllegalArgumentException if an invalid id is provided
	 */
	public static final EdgeTypeGroup of(int id, String name) {
		Preconditions.checkArgument(id>0,"Id must be bigger than 0");
		Preconditions.checkArgument(id<=MAX_GROUP_ID,"Group id must be smaller than or equal to " + MAX_GROUP_ID);
		return new StandardEdgeTypeGroup((short)id,name);
	}

    public static final EdgeTypeGroup getDefaultGroup() {
        return DefaultGroup;
    }

	/**
	 * Returns the name of the edge type group
	 * @return Name of the edge type group
	 */
	public abstract String getName();
	
	/**
	 * Returns the id of the edge type group
	 * @return ID of the edge type group
	 */
	public abstract short getID();
	
}
