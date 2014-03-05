package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.BaseVertexQuery;
import com.thinkaurelius.titan.core.TitanType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPQueryBuilder<S> extends BaseVertexQuery {

    public OLAPJobBuilder<S> edges();

    public OLAPJobBuilder<S> properties();

    public OLAPJobBuilder<S> relations();

    //######## COPIED TO OVERWRITE RESULT ##########

    @Override
    public OLAPQueryBuilder<S> labels(String... labels);

    @Override
    public OLAPQueryBuilder<S> types(TitanType... type);

    @Override
    public OLAPQueryBuilder<S> keys(String... keys);

    @Override
    public OLAPQueryBuilder<S> direction(Direction d);

    @Override
    public OLAPQueryBuilder<S> has(String key);

    @Override
    public OLAPQueryBuilder<S> hasNot(String key);

    @Override
    public OLAPQueryBuilder<S> has(String type, Object value);

    @Override
    public OLAPQueryBuilder<S> hasNot(String key, Object value);

    @Override
    public OLAPQueryBuilder<S> has(String key, Predicate predicate, Object value);

    @Override
    public <T extends Comparable<?>> OLAPQueryBuilder<S> interval(String key, T start, T end);

    @Override
    public OLAPQueryBuilder<S> limit(int limit);



}
