// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core;


import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.util.datastructures.Removable;

/**
 * JanusGraphElement represents the abstract concept of an entity in the graph and specifies basic methods for interacting
 * with entities.
 * The two basic entities in a graph database are {@link JanusGraphRelation} and {@link JanusGraphVertex}.
 * Entities have a life cycle state which reflects the current state of the entity with respect
 * to the {@link JanusGraphTransaction} in which it occurs. An entity may be in any one of these states
 * and any given time:
 * <ul>
 * <li><strong>New:</strong> The entity has been created in the current transaction</li>
 * <li><strong>Loaded:</strong> The entity has been loaded from disk into the current transaction and has not been modified</li>
 * <li><strong>Modified:</strong> The entity has been loaded from disk into the current transaction and has been modified</li>
 * <li><strong>Deleted:</strong> The entity has been deleted in the current transaction</li>
 * </ul>
 * Depending on the concrete type of the entity, an entity may be identifiable,
 * i.e. it has a unique ID which can be retrieved via {@link #id}
 * (use {@link #hasId} to determine if a given entity has a unique ID).
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see JanusGraphVertex
 * @see JanusGraphRelation
 */
public interface JanusGraphElement extends Element, Idfiable, Removable {

    @Override
    JanusGraphTransaction graph();

    /**
     * Returns a unique identifier for this entity.
     * <p>
     * The unique identifier may only be set when the transaction in which entity is created commits.
     * Some entities are never assigned a unique identifier if they depend on a parent entity.
     * <p>
     * JanusGraph allocates blocks of identifiers and automatically assigns identifiers to elements
     * automatically be default.  This behavior can be partially overridden by setting
     * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#ALLOW_SETTING_VERTEX_ID}
     *
     * @return The unique identifier for this entity
     * @throws IllegalStateException if the entity does not (yet) have a unique identifier
     * @see #hasId
     */
    @Override
    default Object id() {
        return longId();
    }

    /**
     * Unique identifier for this entity. This id can be temporarily assigned and might change.
     * Use {@link #id()} for the permanent id.
     *
     * @return Unique long id
     */
    @Override
    long longId();

    /**
     * Checks whether this entity has a unique identifier.
     * <p>
     * Note that some entities may never be assigned an identifier and others will only be
     * assigned an identifier at the end of a transaction.
     *
     * @return true if this entity has been assigned a unique id, else false
     * @see #longId()
     */
    boolean hasId();

    /**
     * Deletes this entity and any incident edges or properties from the graph.
     * <p>
     *
     * @throws IllegalStateException if the entity cannot be deleted or if the user does not
     *                               have permission to remove the entity
     */
    @Override
    void remove();

    /**
     * Sets the value for the given key on this element.
     * The key must be defined to have {@link Cardinality#SINGLE}, otherwise this method throws an exception.
     *
     * @param key   the string identifying the key
     * @param value the object value
     */
    @Override
    <V> Property<V> property(String key, V value);

    /**
     * Retrieves the value associated with the given key on this element and casts it to the specified type.
     * If the key has cardinality SINGLE, then there can be at most one value and this value is returned (or null).
     * Otherwise a list of all associated values is returned, or an empty list if non exist.
     * <p>
     *
     * @param key key
     * @return value or list of values associated with key
     */
    <V> V valueOrNull(PropertyKey key);

    //########### LifeCycle Status ##########

    /**
     * Checks whether this entity has been newly created in the current transaction.
     *
     * @return True, if entity has been newly created, else false.
     */
    boolean isNew();

    /**
     * Checks whether this entity has been loaded into the current transaction and not yet modified.
     *
     * @return True, has been loaded and not modified, else false.
     */
    boolean isLoaded();

    /**
     * Checks whether this entity has been deleted into the current transaction.
     *
     * @return True, has been deleted, else false.
     */
    boolean isRemoved();

}
