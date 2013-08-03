
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.*;

/**
 * TitanQuery constructs and executes a query over incident edges from the perspective of a vertex.
 * <p/>
 * A TitanQuery extends Blueprint's {@link Query} by some Titan specific convenience methods. Using TitanQuery proceeds
 * in two steps: 1) Define the query by specifying what to retrieve and 2) execute the query.
 * <br />
 * A TitanQuery is initialized by calling {@link com.thinkaurelius.titan.core.TitanVertex#query()} on the vertex itself.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface TitanVertexQuery extends VertexQuery {

    /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    /**
     * Query for only those relations matching one of the given types.
     * By default, a query includes all relations in the result set.
     *
     * @param type types to query for
     * @return this query
     */
    public TitanVertexQuery types(TitanType... type);

    /**
     * Query for only those edges matching one of the given labels.
     * By default, an edge query includes all edges in the result set.
     *
     * @param labels edge labels to query for
     * @return this query
     */
    @Override
    public TitanVertexQuery labels(String... labels);

    /**
     * Query for only those properties having one of the given keys.
     * By default, a query includes all properties in the result set.
     *
     * @param keys property keys to query for
     * @return this query
     */
    public TitanVertexQuery keys(String... keys);

    /**
     * Query only for relations in the given direction.
     * By default, both directions are queried.
     *
     * @param d Direction to query for
     * @return this query
     */
    @Override
    public TitanVertexQuery direction(Direction d);

    /**
     * Query only for edges that have an incident property matching the given value.
     * <p/>
     *
     * @param key  key
     * @param value Value for the property of the given key to match
     * @return this query
     */
    public TitanVertexQuery has(TitanKey key, Object value);

    /**
     * Query only for edges that have a unidirected edge matching pointing to the given vertex
     * <p/>
     * It is expected that this label is unidirected ({@link com.thinkaurelius.titan.core.TitanLabel#isUnidirected()}
     * and the query is restricted to edges having an incident unidirectional edge pointing to the given vertex.
     *
     * @param label  Label
     * @param vertex Vertex to point unidirectional edge to
     * @return this query
     */
    public TitanVertexQuery has(TitanLabel label, TitanVertex vertex);

    /**
     * Query only for edges that have an incident property or unidirected edge matching the given value.
     * <p/>
     * If type is a property key, then the query is restricted to edges having an incident property matching
     * this key-value pair.
     * If type is an edge label, then it is expected that this label is unidirected ({@link com.thinkaurelius.titan.core.TitanLabel#isUnidirected()}
     * and the query is restricted to edges having an incident unidirectional edge pointing to the value which is
     * expected to be a {@link TitanVertex}.
     *
     * @param type  TitanType name
     * @param value Value for the property of the given key to match, or vertex to point unidirectional edge to
     * @return this query
     */
    @Override
    public TitanVertexQuery has(String type, Object value);


    @Override
    public TitanVertexQuery hasNot(String key, Object value);

    @Override
    public TitanVertexQuery has(String key, Predicate predicate, Object value);

    public TitanVertexQuery has(TitanKey key, Predicate predicate, Object value);

    @Override
    public TitanVertexQuery has(String key);

    @Override
    public TitanVertexQuery hasNot(String key);

    @Override
    @Deprecated
    public <T extends java.lang.Comparable<T>> TitanVertexQuery has(java.lang.String s, T t, Query.Compare compare);

    /**
     * Query for those edges that have an incident property whose values lies in the interval by [start,end).
     *
     * @param key   property key
     * @param start value defining the start of the interval (inclusive)
     * @param end   value defining the end of the interval (exclusive)
     * @return this query
     */
    @Override
    public <T extends Comparable<?>> TitanVertexQuery interval(String key, T start, T end);

    /**
     * Query for those edges that have an incident property whose values lies in the interval by [start,end).
     *
     * @param key   property key
     * @param start value defining the start of the interval (inclusive)
     * @param end   value defining the end of the interval (exclusive)
     * @return this query
     */
    public <T extends Comparable<?>> TitanVertexQuery interval(TitanKey key, T start, T end);

    /**
     * Sets the retrieval limit for this query.
     * <p/>
     * When setting a limit, executing this query will only retrieve the specified number of relations. Note, that this
     * also applies to counts.
     *
     * @param limit maximum number of relations to retrieve for this query
     * @return this query
     */
    @Override
    public TitanVertexQuery limit(int limit);

    /* ---------------------------------------------------------------
    * Query execution
    * ---------------------------------------------------------------
    */

    /**
     * Returns an iterable over all incident edges that match this query
     *
     * @return Iterable over all incident edges that match this query
     */
    public Iterable<Edge> edges();

    public Iterable<TitanEdge> titanEdges();

    /**
     * Returns an iterable over all incident properties that match this query
     *
     * @return Iterable over all incident properties that match this query
     */
    public Iterable<TitanProperty> properties();


    /**
     * Returns an iterable over all incident relations that match this query
     *
     * @return Iterable over all incident relations that match this query
     */
    public Iterable<TitanRelation> relations();


    /**
     * Returns the number of edges that match this query
     *
     * @return Number of edges that match this query
     */
    public long count();

    /**
     * Returns the number of properties that match this query
     *
     * @return Number of properties that match this query
     */
    public long propertyCount();


    /**
     * Retrieves all vertices connected to this query's central vertex by edges
     * matching the conditions defined in this query.
     * <p/>
     * No guarantee is made as to the order in which the vertices are listed. Use {@link com.thinkaurelius.titan.core.VertexList#sort()}
     * to sort by vertex idAuthorities most efficiently.
     * <p/>
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices connected to this query's central vertex by matching edges
     */
    public VertexList vertexIds();


}
