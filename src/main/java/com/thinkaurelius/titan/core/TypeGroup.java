
package com.thinkaurelius.titan.core;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.types.group.StandardTypeGroup;

/**
 * TypeGroup defines a group of {@link TitanType}s. Grouping TitanTypes into a TypeGroup has the benefit
 * that all relations whose type is in a group can be retrieved at once using {@link TitanQuery#group(TypeGroup)}.
 *
 * For example, one could define the edge labels <i>father</i>, <i>mother</i>, <i>sibling</i> to be in the TypeGroup
 * <i>family</i>. This would allow the retrieval of all father,mother and sibling edges for a given vertex
 * with one database call.
 * <br />
 * TitanTypes are assigned to TypeGroups when they are first created using {@link TypeMaker#group(TypeGroup)}.
 * <br />
 * A TitanGroup is defined with a name and an id, however, two groups with the same id are considered equivalent.
 * The name is only used for recognition has is not persisted in the database. Group ids must be positive (>0)
 * and the maximum group id allowed is configurable.
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 *
 */
public abstract class TypeGroup {
	
    private static final int MAX_GROUP_ID = (2<<6)-2;
    
	/**
	 * The default type group when no group is specified during type construction.
     *
     * Note: system types have group with id 0 which may not be used.
	 * @see TypeMaker
	 */
	public final static TypeGroup DEFAULT_GROUP = of((short)1,"Default Group");
	
	protected TypeGroup() {};
	
	/**
	 * Creates and returns a new type group with the specified id and name
	 * @param id ID of the type group
	 * @param name Name of the type group
	 * @return A type group
	 * @throws IllegalArgumentException if an invalid id is provided
	 */
	public static final TypeGroup of(int id, String name) {
		Preconditions.checkArgument(id>0,"Id must be bigger than 0");
		Preconditions.checkArgument(id<=MAX_GROUP_ID,"Group id must be smaller than or equal to " + MAX_GROUP_ID);
		return new StandardTypeGroup((short)id,name);
	}

    /**
     * Returns the default type group.
     * @return default type group
     */
    public static final TypeGroup getDefaultGroup() {
        return DEFAULT_GROUP;
    }

	/**
	 * Returns the name of the type group
	 * @return Name of the type group
	 */
	public abstract String getName();
	
	/**
	 * Returns the id of the type group
	 * @return ID of the type group
	 */
	public abstract short getID();
	
}
