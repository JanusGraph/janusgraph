package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Order;

/**
 * Constructs a query against a mixed index to retrieve all elements (either vertices or edges)
 * that match all conditions.
 * <p/>
 * Finding matching elements efficiently using this query mechanism requires that appropriate index structures have
 * been defined for the keys. See {@link com.thinkaurelius.titan.core.schema.TitanManagement} for more information
 * on how to define index structures in Titan.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @since 0.3.0
 */

public interface TitanGraphQuery<Q extends TitanGraphQuery<Q>> {

   /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key       Key that identifies the property
     * @param predicate Relation between property and condition
     * @param condition
     * @return This query
     */
    public Q has(String key, TitanPredicate predicate, Object condition);

    public Q has(String key);

    public Q hasNot(String key);

    public Q has(String key, Object value);

    public Q hasNot(String key, Object value);

    public <T extends Comparable<?>> Q interval(String key, T startValue, T endValue);

    /**
     * Limits the size of the returned result set
     *
     * @param max The maximum number of results to return
     * @return This query
     */
    public Q limit(final int max);

    /**
     * Orders the element results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     * @return
     */
    public Q orderBy(String key, Order order);


    /* ---------------------------------------------------------------
    * Query Execution
    * ---------------------------------------------------------------
    */

    /**
     * Returns all vertices that match the conditions.
     *
     * @return
     */
    public Iterable<TitanVertex> vertices();

    /**
     * Returns all edges that match the conditions.
     *
     * @return
     */
    public Iterable<TitanEdge> edges();

    /**
     * Returns all properties that match the conditions
     *
     * @return
     */
    public Iterable<TitanVertexProperty> properties();


}
