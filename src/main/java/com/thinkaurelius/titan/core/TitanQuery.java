
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;

/**
 * TitanQuery constructs and executes a query over incident edges for a fixed node.
 *
 * Using TitanQuery consists of two parts:
 * <ol>
 * <li>Defining the query by specifying conditions on the edges to retrieve via the methods {@link #types},
 * {@link #direction}, {@link #onlyModifiable()}.</li>
 * <li>Executing the query by asking for an {@link Iterable}, {@link java.util.Iterator}, or getting the number of edges that match the query</li>
 * </ol>
 *
 * It is important to note, that TitanQuery only queries the edges incident on a fixed node that was specified when the TitanQuery was
 * instantiated (e.g. through the transaction's {@link TitanTransaction#query(long)} method).
 *
 *
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 *
 *
 *
 */
public interface TitanQuery extends Query {

	/**
	 * Defines this edge query to query only for edges of the specified type.
	 * By default, an edge query includes all edges in the result set.
	 *
	 * @param type TitanRelation type to query for
	 * @return This edge query
	 */
	public TitanQuery types(TitanType... type);

    /**
     * Defines this edge query to query only for edges of the specified type.
     * By default, an edge query includes all edges in the result set.
     *
     * @param labels TitanRelation type to query for
     * @return This edge query
     */
    public TitanQuery labels(String... labels);


    public TitanQuery keys(String... keys);

	/**
	 * Defines this edge query to query only for edges which type belongs to the specified group.
	 * Note that an TypeGroup is uniquely defined by its id. By default, all groups are considered.
	 *
	 * @param group TypeGroup to query edges for
	 * @return This edge query
	 * @see TypeGroup
	 */
	public TitanQuery group(TypeGroup group);

	/**
	 * Defines this edge query to query only for edges in the given direction
	 *
	 * @param d Direction to query for
	 * @return This edge query
	 */
	public TitanQuery direction(Direction d);

    /**
     * Defines this edge query to query only for edges that have an attached (i.e. incident)
     * property or edge of the given type with the specified value or node.
     *
     * @param etype TitanRelation type
     * @param value Value for the property of the given type to match
     * @return This edge query
     */
    public TitanQuery has(TitanType etype, Object value);

    /**
     * Defines this edge query to query only for edges that have an attached (i.e. incident)
     * property or edge of the given type with the specified value or node.
     *
     * @param etype TitanRelation type
     * @param value Value for the property of the given type to match
     * @return This edge query
     */
    public TitanQuery has(String etype, Object value);

    /**
     * Defines this edge query to query only for edges that have an attached (i.e. incident)
     * property of the given type with value in the specified interval.
     *
     * @param ptype TitanProperty type
     * @param start TitanProperty value defining the start of the interval (inclusive)
     * @param end TitanProperty value defining the end of the interval (exclusive)
     * @return This edge query
     */
    public<T extends Comparable<T>> TitanQuery interval(String ptype, T start, T end);

    /**
     * Defines this edge query to query only for edges that have an attached (i.e. incident)
     * property of the given type with value in the specified interval.
     *
     * @param ptype TitanProperty type
     * @param start TitanProperty value defining the start of the interval (inclusive)
     * @param end TitanProperty value defining the end of the interval (exclusive)
     * @return This edge query
     */
    public<T extends Comparable<T>> TitanQuery interval(TitanKey ptype, T start, T end);

	/**
	 * Defines this edge query to query only for modifiable edges.
     * By default, an edge query considers all edges
	 *
	 * @return This edge query
	 */
	public TitanQuery onlyModifiable();

    /**
     * Configures the edge query such that all outgoing edges are first loaded
     * into memory.
     *
     * By default, an implementation of TitanQuery chooses the most efficient way to retrieve a node's edge.
     * For the case of a neighborhood query, the most efficient way is retrieving the neighborhood directly from the hasIndex
     * without loading any edges into memory. However, when repeatedly querying the same or similar node neighborhoods,
     * first loading the edges into memory and using them to answer subsequent queries can be more efficient.
     * Calling this method signals such cases to the TitanQuery implementation.
     *
     * By default, the in memory retrieval flag is false.
     *
     * @return This edge query
     */
    public TitanQuery inMemory();


    /**
     * Sets the retrieval limit for this query.
     *
     * When setting a limit, executing this query will only retrieve the specified number of edges from (external) memory.
     *
     * @param limit Maximum number of edges to retrieve for this edge query
     * @return This edge query
     */
    public TitanQuery limit(long limit);


	/**
	 * Returns an iterable over all incident relationships that match this query
	 *
	 * @return Iterable over all incident relationships that match this query
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
	 * Returns an iterable over all incident edges that match this query
	 *
	 * @return Iterable over all incident edges that match this query
	 */
	public Iterable<TitanRelation> relations();


	/**
	 * Returns the number of relationships that match this query
	 *
	 * @return Number of relationships that match this query
	 */
	public long count();


    public long propertyCount();


    /**
     * Retrieves all nodes connected to this query's fixed node by edges
     * matching the conditions defined in this query.
     *
     * No guarantee is made as to the order in which the nodes are listed. However, in some cases the retrieved list of node ids will be
     * ordered (use {@link VertexList#sort()} to check whether the list is sorted).
     *
     * The query engine will determine the most efficient way to retrieve the nodes that match this query. For INSTANCE,
     * it might only retrieve the node ids and instantiate the node objects only as needed.
     *
     * @return A list of all nodes connected to this query's fixed node by matching edges
     */
    public VertexList vertexIds();

}
