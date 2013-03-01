package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;

/**
 * Constructs a query against an external index to retrieve all elements (either vertices or edges)
 * that match all key conditions.
 *
 * Finding matching elements using this query mechanism requires that appropriate index structures have
 * been defined for the keys. See {@link TypeMaker#indexed(Class)} and {@link TypeMaker#indexed(String, Class)}.
 *
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface ElementQuery {

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key Key that identifies the property
     * @param relation Relation between property and condition
     * @param condition
     * @return This element query
     */
    public ElementQuery and(String key, Relation relation, Object condition);

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key Key that identifies the property
     * @param relation Relation between property and condition
     * @param condition
     * @return This element query
     */
    public ElementQuery and(TitanKey key, Relation relation, Object condition);

    /**
     * Returns all vertices that match the conditions.
     * @return
     */
    public Iterable<TitanVertex> getVertices();

    /**
     * Returns all edges that match the conditions.
     * @return
     */
    public Iterable<TitanEdge> getEdges();

}
