
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;

/**
 * BaseVertexQuery constructs and executes a query over incident edges or properties from the perspective of a vertex.
 * <p/>
 * A VertexQuery extends Blueprint's {@link com.tinkerpop.blueprints.Query} by some Titan specific convenience methods
 * for querying for incident edges or properties.
 * Using VertexQuery proceeds in two steps:
 * 1) Define the query by specifying what to retrieve and
 * 2) execute the query for the elements to retrieve.
 * <p />
 * This is the base interface for the specific implementations of a VertexQuery. Calling {@link com.thinkaurelius.titan.core.TitanVertex#query()}
 * returns a {@link TitanVertexQuery} for querying a single vertex.
 * Calling {@link TitanTransaction#multiQuery(java.util.Collection)} returns a {@link TitanMultiVertexQuery} to execute
 * the same query against multiple vertices at the same time which is typically faster.
 *
 * @see TitanVertexQuery
 * @see TitanMultiVertexQuery
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface BaseVertexQuery<Q extends BaseVertexQuery<Q>> {

    /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    /**
     * Restricts this query to only those edges that point to the given vertex.
     *
     * @param vertex
     * @return this query builder
     */
    public Q adjacent(TitanVertex vertex);

    /**
     * Query for only those relations matching one of the given relation types.
     * By default, a query includes all relations in the result set.
     *
     * @param type relation types to query for
     * @return this query
     */
    public Q types(RelationType... type);

    /**
     * Query for only those edges matching one of the given edge labels.
     * By default, an edge query includes all edges in the result set.
     *
     * @param labels edge labels to query for
     * @return this query
     */
    public Q labels(String... labels);

    /**
     * Query for only those properties having one of the given property keys.
     * By default, a query includes all properties in the result set.
     *
     * @param keys property keys to query for
     * @return this query
     */
    public Q keys(String... keys);

    /**
     * Query only for relations in the given direction.
     * By default, both directions are queried.
     *
     * @param d Direction to query for
     * @return this query
     */
    public Q direction(Direction d);

    /**
     * Query only for edges or properties that have an incident property matching the given value.
     * <p/>
     *
     * @param key  key
     * @param value Value for the property of the given key to match
     * @return this query
     */
    public Q has(PropertyKey key, Object value);

    /**
     * Query only for edges or properties that have a unidirected edge pointing to the given vertex
     * <p/>
     * It is expected that this label is unidirected ({@link EdgeLabel#isUnidirected()}
     * and the query is restricted to edges or properties having an incident unidirectional edge pointing to the given vertex.
     *
     * @param label  Label
     * @param vertex Vertex to point unidirectional edge to
     * @return this query
     */
    public Q has(EdgeLabel label, TitanVertex vertex);

    /**
     * Query for edges or properties that have defined property with the given key
     *
     * @param key
     * @return this query
     */
    public Q has(String key);

    /**
     * Query for edges or properties that DO NOT have a defined property with the given key
     *
     * @param key
     * @return this query
     */
    public Q hasNot(String key);

    /**
     * Query only for edges or properties that have an incident property or unidirected edge matching the given value.
     * <p/>
     * If type is a property key, then the query is restricted to edges or properties having an incident property matching
     * this key-value pair.
     * If type is an edge label, then it is expected that this label is unidirected ({@link EdgeLabel#isUnidirected()}
     * and the query is restricted to edges or properties having an incident unidirectional edge pointing to the value which is
     * expected to be a {@link com.thinkaurelius.titan.core.TitanVertex}.
     *
     * @param type  TitanType name
     * @param value Value for the property of the given key to match, or vertex to point unidirectional edge to
     * @return this query
     */
    public Q has(String type, Object value);

    /**
     * Identical to {@link #has(String, Object)} but negates the condition, i.e. matches those edges or properties
     * that DO NOT satisfy this property condition.
     *
     * @param key
     * @param value
     * @return
     */
    public Q hasNot(String key, Object value);

    /**
     * Query only for those edges or properties which have a property for the given key that satisfies the specified
     * predicate relationship to the provided value.
     *
     * @param key property key
     * @param predicate boolean relationship to satisfy with the given value
     * @param value value
     * @return
     */
    public Q has(PropertyKey key, Predicate predicate, Object value);

    /**
     * Query only for those edges or properties which have a property for the given key that satisfies the specified
     * predicate relationship to the provided value.
     *
     * @param key property key
     * @param predicate boolean relationship to satisfy with the given value
     * @param value value
     * @return
     */
    public Q has(String key, Predicate predicate, Object value);

    /**
     * Query for those edges or properties that have a property for the given key
     * whose values lies in the interval by [start,end).
     *
     * @param key   property key
     * @param start value defining the start of the interval (inclusive)
     * @param end   value defining the end of the interval (exclusive)
     * @return this query
     */
    public <T extends Comparable<?>> Q interval(String key, T start, T end);

    /**
     * Query for those edges or properties that have a property for the given key
     * whose values lies in the interval by [start,end).
     *
     * @param key   property key
     * @param start value defining the start of the interval (inclusive)
     * @param end   value defining the end of the interval (exclusive)
     * @return this query
     */
    public <T extends Comparable<?>> Q interval(PropertyKey key, T start, T end);

    /**
     * Sets the retrieval limit for this query.
     * <p/>
     * When setting a limit, executing this query will only retrieve the specified number of relations. Note, that this
     * also applies to counts.
     *
     * @param limit maximum number of relations to retrieve for this query
     * @return this query
     */
    public Q limit(int limit);


    /**
     * Orders the relation results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     * </p>
     * Note, that the ordering always applies to the incident relations (edges/properties) and NOT
     * to the adjacent vertices even if only vertices are being returned.
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     * @return
     */
    public Q orderBy(String key, Order order);

    /**
     * Orders the relation results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     * </p>
     * Note, that the ordering always applies to the incident relations (edges/properties) and NOT
     * to the adjacent vertices even if only vertices are being returned.
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     * @return
     */
    public Q orderBy(PropertyKey key, Order order);


    /* ---------------------------------------------------------------
    * Query Execution
    * ---------------------------------------------------------------
    */


    /**
     * Returns a description of this query for edges as a {@link QueryDescription} object.
     *
     * This can be used to inspect the query plan for this query. Note, that calling this method
     * does not actually execute the query but only optimizes it and constructs a query plan.
     *
     * @return A description of this query for edges
     */
    public QueryDescription describeForEdges();

    /**
     * Returns a description of this query for properties as a {@link QueryDescription} object.
     *
     * This can be used to inspect the query plan for this query. Note, that calling this method
     * does not actually execute the query but only optimizes it and constructs a query plan.
     *
     * @return A description of this query for properties
     */
    public QueryDescription describeForProperties();

}
