package com.thinkaurelius.titan.core.olap;

import com.google.common.base.Function;
import com.thinkaurelius.titan.core.*;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;

/**
 * Builds a vertex-centric query to define the edges and/or properties that will be retrieved for each vertex as part
 * of the OLAP operation and aggregated into a single property on the central vertex which is accessible via {@link TitanVertex#getProperty(String)}
 * in the {@link OLAPJob}.
 * <p/>
 * This query builder, just like {@link BaseVertexQuery}, allows the specification of the edges and properties to be received.
 * However, unlike an OLTP query, the edges are not returned individually but aggregated into one state using the {@link Gather}
 * and {@link Combiner} functions specified in {@link #edges(Gather, Combiner)} and {@link #properties(com.google.common.base.Function, Combiner)},
 * respectively.
 * That state, is then accessible as a property on the central vertex. The key to access that property is the name defined
 * in the {@link #setName(String)} method. If no name is explicitly set, the name of the edge label or property key is used
 * if only one such type is specified - otherwise a missing name exception is thrown.
 * <p/>
 * In essence, this query defines a subset of each vertex's adjacency list along which to aggregate state information which
 * can include the adjacent vertex's state as well as any edge properties.
 * This query defines the vertex's adjacency sub-list and how to aggregate the state.
 *
 * @see TitanVertexQuery
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPQueryBuilder<S,M,Q extends OLAPQueryBuilder<S,M,Q>> extends BaseVertexQuery<Q> {


    /**
     * Set's the name for this query. The queries aggregate result will be accessible as a property on each vertex in an
     * {@link OLAPJob} using this name as the key. Hence, it should be ensured that this name is unique and does not conflict
     * with the names of any types defined in the graph.
     * <p/>
     * A name must always be specified, unless this query retrieves only one type in which case the name of that type will
     * be used by default unless explicitly overwritten with this method.
     *
     * @param name Name for this query
     * @return
     */
    public Q setName(String name);

    /**
     * Defines an adjacency aggregate over the edges identified by this query. The gather function is applied to
     * all edges and the adjacent vertex's state in that result set and the returned states are combined with the specified combiner.
     *
     * @param gather Function used to gather the edges and adjacent states
     * @param combiner Function used to combine the gathered states
     * @param <M>
     * @return the builder for this OLAPJob
     */
    public<M> OLAPJobBuilder<S> edges(Gather<S,M> gather, Combiner<M> combiner);

    /**
     * Identical to {@link #edges(Gather, Combiner)} using the default gather function which simply returns the adjacent
     * vertex's state.
     *
     * @param combiner Combiner method for combining gathered messages
     * @return the builder for this OLAPJob
     */
    public OLAPJobBuilder<S> edges(Combiner<S> combiner);


    /**
     * Defines an adjacency aggregate over the properties identified by this query. The gather function is applied to
     * all properties in that result set and the returned states are combined with the specified combiner.
     *
     * @param gather Function used to gather the properties
     * @param combiner Function used to combine the gathered states
     * @param <M>
     * @return the builder for this OLAPJob
     */
    public<M> OLAPJobBuilder<S> properties(Function<TitanProperty,M> gather, Combiner<M> combiner);

    /**
     * Identical to {@link #properties(com.google.common.base.Function, Combiner)} using a default gather function which
     * simply returns the value of the property.
     *
     * @param combiner Combiner method for combining the values of all adjacent properties.
     * @return the builder for this OLAPJob
     */
    public OLAPJobBuilder<S> properties(Combiner<Object> combiner);

    /**
     * Identical to {@link #properties(com.google.common.base.Function, Combiner)} using default gather and combiner functions.
     * The gather function retrieves the values of the adjacent properties.
     * If this query only specifies one single-valued property key, the combiner function just returns that one value.
     * If multiple values are possible, the combiner function aggregates those values into one list.
     *
     * @return the builder for this OLAPJob
     */
    public OLAPJobBuilder<S> properties();


   /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    @Override
    public Q adjacent(TitanVertex vertex);

    @Override
    public Q types(RelationType... type);

    @Override
    public Q labels(String... labels);

    @Override
    public Q keys(String... keys);

    @Override
    public Q direction(Direction d);

    @Override
    public Q has(PropertyKey key, Object value);

    @Override
    public Q has(EdgeLabel label, TitanVertex vertex);

    @Override
    public Q has(String key);

    @Override
    public Q hasNot(String key);

    @Override
    public Q has(String type, Object value);

    @Override
    public Q hasNot(String key, Object value);


    @Override
    public Q has(PropertyKey key, Predicate predicate, Object value);

    @Override
    public Q has(String key, Predicate predicate, Object value);

    @Override
    public <T extends Comparable<?>> Q interval(String key, T start, T end);

    @Override
    public <T extends Comparable<?>> Q interval(PropertyKey key, T start, T end);

    @Override
    public Q limit(int limit);

    @Override
    public Q orderBy(String key, Order order);

    @Override
    public Q orderBy(PropertyKey key, Order order);


}
