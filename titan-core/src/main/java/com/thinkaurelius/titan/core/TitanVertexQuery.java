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
public interface TitanVertexQuery extends BaseVertexQuery, VertexQuery {

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
    public TitanVertexQuery adjacentVertex(TitanVertex vertex);

    @Override
    public TitanVertexQuery labels(String... labels);

    @Override
    public TitanVertexQuery types(TitanType... type);

    @Override
    public TitanVertexQuery direction(Direction d);

    @Override
    public TitanVertexQuery has(String key);

    @Override
    public TitanVertexQuery hasNot(String key);

    @Override
    public TitanVertexQuery has(String type, Object value);

    @Override
    public TitanVertexQuery hasNot(String key, Object value);

    @Override
    public TitanVertexQuery has(String key, Predicate predicate, Object value);

    @Override
    @Deprecated
    public <T extends Comparable<T>> TitanVertexQuery has(String s, T t, Compare compare);

    @Override
    public <T extends Comparable<?>> TitanVertexQuery interval(String key, T start, T end);

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

    /**
     * Returns an iterable over all incident edges that match this query. Returns edges as {@link TitanEdge}.
     *
     * @return Iterable over all incident edges that match this query
     */
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
