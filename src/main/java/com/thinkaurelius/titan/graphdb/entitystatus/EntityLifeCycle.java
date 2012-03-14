package com.thinkaurelius.titan.graphdb.entitystatus;

/**
 * EntityLifeCycle enumerates all possible states of the lifecycle of a entity.
 * 
 * @author Matthias Broecheler (me@matthiasb.com);
 *
 */
public class EntityLifeCycle {

	/** 
	 * The entity has been newly created and not yet persisted.
	 */
	final static byte New = 1;

	
	/**
	 * The entity has been loaded from the database and has not changed
	 * after initial loading.
	 */
	final static byte Loaded = 2;
	
	/**
	 * The entity has changed after being loaded from the database.
	 */
	final static byte Modified = 3;
	
	/**
	 * The entity has been deleted but not yet erased from the database.
	 */
	final static byte Deleted = 4;

	
}
