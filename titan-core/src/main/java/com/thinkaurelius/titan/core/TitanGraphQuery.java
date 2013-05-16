package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

/**
 * Constructs a query against an external index to retrieve all elements (either vertices or edges)
 * that match all conditions.
 *
 * Finding matching elements using this query mechanism requires that appropriate index structures have
 * been defined for the keys. See {@link TypeMaker#indexed(Class)} and {@link TypeMaker#indexed(String, Class)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @since 0.3.0
 */

public interface TitanGraphQuery extends GraphQuery {

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key Key that identifies the property
     * @param relation Relation between property and condition
     * @param condition
     * @return This query
     */
    public TitanGraphQuery has(String key, Relation relation, Object condition);

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key Key that identifies the property
     * @param relation Relation between property and condition
     * @param value
     * @return This query
     */
    public TitanGraphQuery has(String key, Query.Compare relation, Object value);


    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key Key that identifies the property
     * @param relation Relation between property and condition
     * @param condition
     * @return This query
     */
    public TitanGraphQuery has(TitanKey key, Relation relation, Object condition);

    /**
     * Returns all vertices that match the conditions.
     * @return
     */
    public Iterable<Vertex> vertices();

    /**
     * Returns all edges that match the conditions.
     * @return
     */
    public Iterable<Edge> edges();


    /**
     * Limits the size of the returned result set
     * @param max The maximum number of results to return
     * @return This query
     */
    public TitanGraphQuery limit(final long max);

}
