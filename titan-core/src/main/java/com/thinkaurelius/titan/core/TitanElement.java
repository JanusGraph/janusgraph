package com.thinkaurelius.titan.core;


import com.thinkaurelius.titan.util.datastructures.Removable;
import com.tinkerpop.blueprints.Element;

/**
 * TitanElement represents the abstract concept of an entity in the graph and specifies basic methods for interacting
 * with entities.
 * The two basic entities in a graph database are {@link TitanRelation} and {@link TitanVertex}.
 * Entities have a life cycle state which reflects the current state of the entity with respect
 * to the {@link TitanTransaction} in which it occurs. An entity may be in any one of these states
 * and any given time:
 * <ul>
 * <li><strong>New:</strong> The entity has been created in the current transaction</li>
 * <li><strong>Loaded:</strong> The entity has been loaded from disk into the current transaction and has not been modified</li>
 * <li><strong>Modified:</strong> The entity has been loaded from disk into the current transaction and has been modified</li>
 * <li><strong>Deleted:</strong> The entity has been deleted in the current transaction</li>
 * </ul>
 * Depending on the concrete type of the entity, an entity may be identifiable,
 * i.e. it has a unique ID which can be retrieved via {@link #getId}
 * (use {@link #hasId} to determine if a given entity has a unique ID).
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanVertex
 * @see TitanRelation
 */
public interface TitanElement extends Element, Comparable<TitanElement>, Removable {

    /**
     * Returns a unique identifier for this entity.
     * <p/>
     * The unique identifier may only be set when the transaction in which entity is created commits.
     * The <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a> explains
     * how to configure when entity idAuthorities are assigned.
     * Some entities are never assigned a unique identifier if they depend on a parent entity.
     * <p/>
     *
     * @return The unique identifier for this entity
     * @throws IllegalStateException if the entity does not (yet) have a unique identifier
     * @see #hasId
     */
    public Object getId();

    /**
     * Unique identifier for this entity. This id can be temporarily assigned and might change.
     * Use {@link #getId()} for the permanent id.
     *
     * @return Unique long id
     */
    public long getID();

    /**
     * Checks whether this entity has a unique identifier.
     * <p/>
     * Note that some entities may never be assigned an identifier and others will only be
     * assigned an identifier at the end of a transaction.
     *
     * @return true if this entity has been assigned a unique id, else false
     * @see #getID()
     */
    public boolean hasId();

    /**
     * Deletes this entity from the graph.
     * <p/>
     * Removing an entity assumes that the entity does not have any incident TitanRelations.
     * Use {@link com.tinkerpop.blueprints.Graph#removeEdge(com.tinkerpop.blueprints.Edge)} or {@link com.tinkerpop.blueprints.Graph#removeEdge(com.tinkerpop.blueprints.Edge)}
     * to remove an entity unconditionally.
     *
     * @throws IllegalStateException if the entity cannot be deleted or if the user does not
     *                               have permission to remove the entity
     */
    public void remove();

    /**
     * Sets the value for the given key on this element.
     * The key must be defined single valued (see {@link com.thinkaurelius.titan.core.KeyMaker#single()}).
     *
     * @param key   the string identifying the key
     * @param value the object value
     */
    public void setProperty(String key, Object value);

    /**
     * Sets the value for the given key on this element.
     * The key must be defined single valued (see {@link com.thinkaurelius.titan.core.KeyMaker#single()}).
     *
     * @param key   the key
     * @param value the object value
     */
    public void setProperty(TitanKey key, Object value);

    /**
     * Retrieves the value associated with the given key on this vertex and casts it to the specified type.
     * If the key is single-valued, then there can be at most one value and this value is returned (or null).
     * If the key is list-valued, then a list of all associated values is returned, or an empty list of non exist.
     * <p/>
     *
     * @param key key
     * @return value or list of values associated with key
     */
    public <O> O getProperty(TitanKey key);

    /**
     * Retrieves the value associated with the given key on this vertex and casts it to the specified type.
     * If the key is single-valued, then there can be at most one value and this value is returned (or null).
     * If the key is list-valued, then a list of all associated values is returned, or an empty list of non exist.
     * <p/>
     *
     * @param key string identifying a key
     * @return value or list of values associated with key
     */
    public <O> O getProperty(String key);

    /**
     * Removes the value associated with the given key for this vertex (if exists).
     *
     * @param key the string identifying the key
     * @return the object value associated with that key prior to removal, or NULL if such does not exist.
     * @see #removeProperty(TitanType)
     */
    public <O> O removeProperty(String key);

    /**
     * Removes the value associated with the given key for this vertex (if exists).
     * If the key is list-valued then all values associated with this key are removed and only
     * the last removed value returned.
     *
     * @param type the key
     * @return the object value associated with that key prior to removal, or NULL if such does not exist.
     */
    public <O> O removeProperty(TitanType type);


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
     * Checks whether this entity has been deleted into the current transaction.
     *
     * @return True, has been deleted, else false.
     */
    public boolean isRemoved();


}
