package com.thinkaurelius.titan.core.olap;

import com.google.common.base.Function;
import com.thinkaurelius.titan.core.*;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;

/**
 * Builds a vertex-centric query to define the edges and/or properties that will be accessible during the execution
 * of an {@link OLAPJob} as defined through the associated {@link OLAPQueryBuilder}.
 * <p/>
 * The query builder is identical to {@link TitanVertexQuery} in how the query is defined. The query is completed
 * by calling either {@link #edges()} or {@link #properties()} to configure this query to retrieve
 * edges, properties, or relations, respectively.
 * <p/>
 * Note, that this query is not executed. It simply defines what types of queries can be answered during the execution
 * of the {@link OLAPJob}. Only the data matching those queries will be loaded and available.
 *
 * @see TitanVertexQuery
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPQueryBuilder<S,M,Q extends OLAPQueryBuilder<S,M,Q>> extends BaseVertexQuery<Q> {

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

    public Q setName(String name);

    /**
     * Adds this query as an edge query to the list of available queries for OLAPJob
     *
     * @return the builder for this OLAPJob
     */
    public OLAPJobBuilder<S> edges(Combiner<S> combiner);

    public<M> OLAPJobBuilder<S> edges(Gather<S,M> gather, Combiner<M> combiner);


    /**
     * Adds this query as a property query to the list of available queries for OLAPJob
     *
     * @return the builder for this OLAPJob
     */
    public<M> OLAPJobBuilder<S> properties(Function<TitanProperty,M> gather, Combiner<M> combiner);

    public OLAPJobBuilder<S> properties(Combiner<Object> combiner);

    public OLAPJobBuilder<S> properties();



}
