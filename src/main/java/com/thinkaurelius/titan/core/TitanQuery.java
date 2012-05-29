
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;

/**
 * TitanQuery constructs and executes a query over incident edges from the perspective of a vertex.
 *
 * A TitanQuery extends Blueprint's {@link Query} by some Titan specific convenience methods. Using TitanQuery proceeds
 * in two steps: 1) Define the query by specifying what to retrieve and 2) execute the query.
 * <br />
 * A TitanQuery is initialized by calling {@link TitanTransaction#query(long)} with an existing vertex id or
 * {@link com.thinkaurelius.titan.core.TitanVertex#query()} on the vertex itself.
 *
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 *
 */
public interface TitanQuery extends Query {

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
	public TitanQuery types(TitanType... type);

    /**
     * Query for only those edges matching one of the given labels.
     * By default, an edge query includes all edges in the result set.
     *
     * @param labels edge labels to query for
     * @return this query
     */
    public TitanQuery labels(String... labels);

    /**
     * Query for only those properties having one of the given keys.
     * By default, a query includes all properties in the result set.
     *
     * @param keys property keys to query for
     * @return this query
     */
    public TitanQuery keys(String... keys);

	/**
	 * Query for only those relations having a type included in the given {@link TypeGroup}.
     *
     * This can be significantly more efficient than specifying all types explicitly.
	 *
	 * @param group group of types to restrict query to.
     * @return this query
	 * @see TypeGroup
	 */
	public TitanQuery group(TypeGroup group);

	/**
	 * Query only for relations in the given direction.
     * By default, both directions are queried.
	 *
	 * @param d Direction to query for
     * @return this query
	 */
	public TitanQuery direction(Direction d);

    /**
     * Query only for edges that have an incident property or unidirected edge matching the given value.
     *
     * If type is a property key, then the query is restricted to edges having an incident property matching
     * this key-value pair.
     * If type is an edge label, then it is expected that this label is unidirected ({@link com.thinkaurelius.titan.core.TitanLabel#isUnidirected()}
     * and the query is restricted to edges having an incident unidirectional edge pointing to the value which is
     * expected to be a {@link TitanVertex}.
     *
     * @param type TitanType
     * @param value Value for the property of the given key to match, or vertex to point unidirectional edge to
     * @return this query
     */
    public TitanQuery has(TitanType type, Object value);

    /**
     * Query only for edges that have an incident property or unidirected edge matching the given value.
     *
     * If type is a property key, then the query is restricted to edges having an incident property matching
     * this key-value pair.
     * If type is an edge label, then it is expected that this label is unidirected ({@link com.thinkaurelius.titan.core.TitanLabel#isUnidirected()}
     * and the query is restricted to edges having an incident unidirectional edge pointing to the value which is
     * expected to be a {@link TitanVertex}.
     *
     * @param type TitanType name
     * @param value Value for the property of the given key to match, or vertex to point unidirectional edge to
     * @return this query
     */
    public TitanQuery has(String type, Object value);

    /**
     * Query for those edges that have an incident property whose values lies in the interval by [start,end).
     *
     * @param key property key
     * @param start value defining the start of the interval (inclusive)
     * @param end value defining the end of the interval (exclusive)
     * @return this query
     */
    public<T extends Comparable<T>> TitanQuery interval(String key, T start, T end);

    /**
     * Query for those edges that have an incident property whose values lies in the interval by [start,end).
     *
     * @param key property key
     * @param start value defining the start of the interval (inclusive)
     * @param end value defining the end of the interval (exclusive)
     * @return this query
     */
    public<T extends Comparable<T>> TitanQuery interval(TitanKey key, T start, T end);

	/**
     * Query for edges that are modifiable.
	 *
     * @return this query
     * @see com.thinkaurelius.titan.core.TitanType#isModifiable()
	 */
	public TitanQuery onlyModifiable();

    /**
     * Configures the query for in-memory retrieval.
     *
     * By default, an implementation of TitanQuery chooses the most efficient way to retrieve a vertex's incident relations.
     * In some cases, it may be most efficient to NOT create the object graph in memory but only retrieve what is asked
     * for (e.g. vertex ids) directly from the storage backend. However, when the same or similar queries are repeatedly
     * executed, creating the object graph in memory first is more efficient.
     * Calling this method signals such cases to the TitanQuery implementation.
     *
     * By default, the in memory retrieval flag is false.
     *
     * @return this query
     */
    public TitanQuery inMemory();


    /**
     * Sets the retrieval limit for this query.
     *
     * When setting a limit, executing this query will only retrieve the specified number of relations. Note, that this
     * also applies to counts.
     *
     * @param limit maximum number of relations to retrieve for this query
     * @return this query
     */
    public TitanQuery limit(long limit);

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
     *
     * No guarantee is made as to the order in which the vertices are listed. Use {@link com.thinkaurelius.titan.core.VertexList#sort()}
     * to sort by vertex ids most efficiently.
     *
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices connected to this query's central vertex by matching edges
     */
    public VertexList vertexIds();

}
