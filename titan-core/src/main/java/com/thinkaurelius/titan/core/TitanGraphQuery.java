package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;

/**
 * Constructs a query against an external index to retrieve all elements (either vertices or edges)
 * that match all conditions.
 * <p/>
 * Finding matching elements using this query mechanism requires that appropriate index structures have
 * been defined for the keys. See {@link KeyMaker#indexed(Class)} and {@link KeyMaker#indexed(String, Class)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @since 0.3.0
 */

public interface TitanGraphQuery extends GraphQuery {

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key       Key that identifies the property
     * @param predicate Predicate between property and condition
     * @param condition
     * @return This query
     */
    @Override
    public TitanGraphQuery has(String key, Predicate predicate, Object condition);

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key       Key that identifies the property
     * @param predicate Relation between property and condition
     * @param condition
     * @return This query
     */
    public TitanGraphQuery has(TitanKey key, TitanPredicate predicate, Object condition);


    @Override
    public TitanGraphQuery has(String key);

    @Override
    public TitanGraphQuery hasNot(String key);

    @Override
    public TitanGraphQuery has(String key, Object value);

    @Override
    public TitanGraphQuery hasNot(String key, Object value);

    @Override
    @Deprecated
    public <T extends Comparable<T>> TitanGraphQuery has(String key, T value, Compare compare);

    @Override
    public <T extends Comparable<?>> TitanGraphQuery interval(String key, T startValue, T endValue);

    /**
     * Returns all vertices that match the conditions.
     *
     * @return
     */
    public Iterable<Vertex> vertices();

    /**
     * Returns all edges that match the conditions.
     *
     * @return
     */
    public Iterable<Edge> edges();


    /**
     * Limits the size of the returned result set
     *
     * @param max The maximum number of results to return
     * @return This query
     */
    @Override
    public TitanGraphQuery limit(final int max);

    /**
     * Orders the element results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     * @return
     */
    public TitanGraphQuery orderBy(String key, Order order);

    /**
     * Orders the element results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     * @return
     */
    public TitanGraphQuery orderBy(TitanKey key, Order order);

}
