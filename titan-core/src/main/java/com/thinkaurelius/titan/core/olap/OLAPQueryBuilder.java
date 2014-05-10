package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.*;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;

/**
 * Builds a vertex-centric query to define the edges and/or properties that will be accessible during the execution
 * of an {@link OLAPJob} as defined through the associated {@link OLAPQueryBuilder}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPQueryBuilder<S,Q extends OLAPQueryBuilder<S,Q>> extends BaseVertexQuery<Q> {

   /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    @Override
    public Q adjacent(TitanVertex vertex);

    @Override
    public Q types(TitanType... type);

    @Override
    public Q labels(String... labels);

    @Override
    public Q keys(String... keys);

    @Override
    public Q direction(Direction d);

    @Override
    public Q has(TitanKey key, Object value);

    @Override
    public Q has(TitanLabel label, TitanVertex vertex);

    @Override
    public Q has(String key);

    @Override
    public Q hasNot(String key);

    @Override
    public Q has(String type, Object value);

    @Override
    public Q hasNot(String key, Object value);


    @Override
    public Q has(TitanKey key, Predicate predicate, Object value);

    @Override
    public Q has(String key, Predicate predicate, Object value);

    @Override
    public <T extends Comparable<?>> Q interval(String key, T start, T end);

    @Override
    public <T extends Comparable<?>> Q interval(TitanKey key, T start, T end);

    @Override
    public Q limit(int limit);

    @Override
    public Q orderBy(String key, Order order);

    @Override
    public Q orderBy(TitanKey key, Order order);

    public OLAPJobBuilder<S> edges();

    public OLAPJobBuilder<S> properties();

    public OLAPJobBuilder<S> relations();


}
