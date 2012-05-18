
package com.thinkaurelius.titan.core;


/***
 * Represents the abstract concept of an entity in the graph and specifies basic methods for interacting
 * with entities.
 * The two basic entities in a graph database are {@link TitanRelation}s and {@link TitanVertex}s.
 * Entities have a life cycle state which reflects the current state of the entity with respect
 * to the {@link TitanTransaction} in which it occurs. An entity may be in any one of these states
 * and any given time:
 * <ul>
 * <li>New: The entity has been created in the current transaction</li>
 * <li>Loaded: The entity has been loaded from disk into the current transaction and has not been modified</li>
 * <li>Modified: The entity has been loaded from disk into the current transaction and has been modified</li>
 * <li>Deleted: The entity has been deleted in the current transaction</li>
 * <li>ReferenceNode: The entity is a reference to a remotely hosted node. It cannot be loaded into the current transaction but
 * only references the actual entity. This only applies when running Titan embedded in a distributed environment.
 * 
 * Depending on the concrete type of the entity, an entity may be identifiable, 
 * i.e. it has a unique ID which can be retrieved via {@link #getID}
 * (use {@link #hasID} to determine if a given entity has a unique ID).
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanElement {

	/**
	 * Returns a unique identifier for this entity. 
	 * 
	 * @return The unique identifier for this entity
	 * @throws IllegalStateException if the entity does not (yet) have a unique identifier
	 * @see #hasID
	 */
	public long getID();
	
	/***
	 * Checks whether this entity has a unique identifier.
	 * 
	 * Note that some entities may never be assigned an identifier and others will only be
	 * assigned an identifier at the end of a transaction.
	 * 
	 * @return true if this entity has been assigned a unique id, else false
	 */
	public boolean hasID();
	
	/**
	 * Deletes this entity from the graph database
	 * 
	 * @throws IllegalStateException if the entity cannot be deleted or if the user does not
	 * have permission to remove the entity
	 */
	public void remove();
	

	//########### LifeCycle Status ##########
	
	/**
	 * Checks whether this entity has been newly created in the current transaction.
	 * 
	 * @return True, if entity has been newly created, else false.
	 */
	public boolean isNew();
	
	/**
	 * Checks whether this entity has been loaded into the current transaction and not yet modified.
	 * 
	 * @return True, has been loaded and not modified, else false.
	 */
	public boolean isLoaded();
	
	/**
	 * Checks whether this entity has been loaded into the current transaction and modified.
	 * 
	 * @return True, has been loaded and modified, else false.
	 */
	public boolean isModified();
		
	/**
	 * Checks whether this entity has been deleted into the current transaction.
	 * 
	 * @return True, has been deleted, else false.
	 */
	public boolean isRemoved();
	
	/**
	 * Checks whether this entity denotes a reference node which cannot be loaded into the current transaction.
	 * 
	 * @return True, if this entity denotes a reference node, else false.
	 */
	public boolean isReferenceNode();
	
	/**
	 * Checks whether this entity is available in the current transaction.
	 * 
	 * An entity is considered available iff it is not deleted and also not a reference node.
	 * In other words, an entity is available if it is new, loaded or modified.
	 * 
	 * @return true, if the entity is available, else false
	 */
	public boolean isAvailable();
	
	/**
	 * If the transaction in which this entity lives is still open.
	 * 
	 * @return true if this entity's transaction is still open, else false.
	 */
	public boolean isAccessible();
	


}
