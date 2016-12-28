package com.thinkaurelius.titan.core;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * A TitanVertexQuery is a VertexQuery executed for a single vertex.
 * <p />
 * Calling {@link com.thinkaurelius.titan.core.TitanVertex#query()} builds such a query against the vertex
 * this method is called on. This query builder provides the methods to specify which indicent edges or
 * properties to query for.
 *
 *
 * @see BaseVertexQuery
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface TitanVertexQuery<Q extends TitanVertexQuery<Q>> extends BaseVertexQuery<Q> {

   /* ---------------------------------------------------------------
    * Query Specification (overwrite to merge BaseVertexQuery with Blueprint's VertexQuery)
    * ---------------------------------------------------------------
    */

    @Override
    public Q adjacent(Vertex vertex);

    @Override
    public Q types(String... type);

    @Override
    public Q types(RelationType... type);

    @Override
    public Q labels(String... labels);

    @Override
    public Q keys(String... keys);

    @Override
    public Q direction(Direction d);

    @Override
    public Q has(String type, Object value);

    @Override
    public Q has(String key);

    @Override
    public Q hasNot(String key);

    @Override
    public Q hasNot(String key, Object value);

    @Override
    public Q has(String key, TitanPredicate predicate, Object value);

    @Override
    public <T extends Comparable<?>> Q interval(String key, T start, T end);

    @Override
    public Q limit(int limit);

    @Override
    public Q orderBy(String key, Order order);


    /* ---------------------------------------------------------------
    * Query execution
    * ---------------------------------------------------------------
    */

    /**
     * Returns an iterable over all incident edges that match this query
     *
     * @return Iterable over all incident edges that match this query
     */
    public Iterable<TitanEdge> edges();


    public Iterable<TitanVertex> vertices();

    /**
     * Returns an iterable over all incident properties that match this query
     *
     * @return Iterable over all incident properties that match this query
     */
    public Iterable<TitanVertexProperty> properties();

    /**
     * Returns an iterable over all incident relations that match this query
     *
     * @return Iterable over all incident relations that match this query
     */
    public Iterable<TitanRelation> relations();

    /**
     * Returns the number of relations that match this query
     *
     * @return Number of relations that match this query
     */
    public default long count() {
        return Iterables.size(relations());
    }

    /**
     * Returns the number of edges that match this query
     *
     * @return Number of edges that match this query
     */
    public default long edgeCount() {
        return vertexIds().size();
    }

    /**
     * Returns the number of properties that match this query
     *
     * @return Number of properties that match this query
     */
    public default long propertyCount() {
        return Iterables.size(properties());
    }

    /**
     * Retrieves all vertices connected to this query's base vertex by edges
     * matching the conditions defined in this query.
     * <p/>
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices connected to this query's base vertex by matching edges
     */
    public VertexList vertexIds();


}
