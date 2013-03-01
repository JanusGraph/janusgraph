
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
     * Add a property with the given key and value to this vertex.
     * If the key is functional, then a possibly existing value is replaced. If it is not functional, then
     * a new property is added.
     *
     * @param key   the string key of the property
     * @param value the object value o the property
     */
    public void setProperty(String key, Object value);

    /**
     * Add a property with the given key and value to this vertex.
     * If the key is functional, then a possibly existing value is replaced. If it is not functional, then
     * a new property is added.
     *
     * @param key   the string key of the property
     * @param value the object value o the property
     */
    public void setProperty(TitanKey key, Object value);

    /**
     * Retrieves the attribute value for the only property of the specified property key incident on this vertex.
     * <p/>
     * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this vertex.
     * If there could be multiple properties (i.e. for non-functional property keys), then use {@link #getProperties(TitanKey)}
     *
     * @param key key of the property for which to retrieve the attribute value
     * @return Attribute value of the property with the specified type or null if no such property exists
     * @throws IllegalArgumentException if more than one property of the specified key are incident on this vertex.
     */
    public Object getProperty(TitanKey key);

    /**
     * Retrieves the attribute value for the only property of the specified property key incident on this vertex.
     * <p/>
     * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this vertex.
     * If there could be multiple properties (i.e. for non-functional property keys), then use {@link #getProperties(String)}
     *
     * @param key key name of the property for which to retrieve the attribute value
     * @return Attribute value of the property with the specified type or null if no such property exists
     * @throws IllegalArgumentException if more than one property of the specified key are incident on this vertex.
     */
    public Object getProperty(String key);

    /**
     * Retrieves the attribute value for the only property of the specified property key incident on this vertex and casts
     * it to the specified {@link Class}.
     * <p/>
     * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this vertex.
     *
     * @param key key name of the property for which to retrieve the attribute value
     * @return Attribute value of the property with the specified type
     * @throws IllegalArgumentException if more than one property of the specified key are incident on this vertex.
     */
    public <O> O getProperty(TitanKey key, Class<O> clazz);

    /**
     * Retrieves the attribute value for the only property of the specified property key incident on this vertex and casts
     * it to the specified {@link Class}.
     * <p/>
     * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this vertex.
     *
     * @param key key of the property for which to retrieve the attribute value
     * @return Attribute value of the property with the specified type
     * @throws IllegalArgumentException if more than one property of the specified key are incident on this vertex.
     */
    public <O> O getProperty(String key, Class<O> clazz);

    /**
     * Removes all properties with the given key from this vertex.
     *
     * @param key the key of the properties to remove from the element
     * @return the object value (or null) associated with that key prior to removal if the key is functional,
     *         or an {@link Iterable} over all values (which may be empty) if it is not functional
     */
    public Object removeProperty(String key);

    public Object removeProperty(TitanType type);


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
