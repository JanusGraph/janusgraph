package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.relations.RelationComparator;
import com.tinkerpop.blueprints.Direction;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexCentricQuery extends BaseVertexCentricQuery implements ElementQuery<TitanRelation, SliceQuery> {

    private final InternalVertex vertex;

    public VertexCentricQuery(InternalVertex vertex, Condition<TitanRelation> condition,
                              Direction direction,
                              List<BackendQueryHolder<SliceQuery>> queries,
                              int limit) {
        super(condition, direction, queries, limit);
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    public VertexCentricQuery(InternalVertex vertex, BaseVertexCentricQuery base) {
        super(base);
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    protected VertexCentricQuery(InternalVertex vertex) {
        super();
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    public static VertexCentricQuery emptyQuery(InternalVertex vertex) {
        return new VertexCentricQuery(vertex);
    }

    public InternalVertex getVertex() {
        return vertex;
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public Comparator getSortOrder() {
        return new RelationComparator(vertex);
    }

    @Override
    public boolean hasDuplicateResults() {
        return false; //We wanna count self-loops twice
    }

    @Override
    public String toString() {
        return vertex+super.toString();
    }

}
