package com.thinkaurelius.titan.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.ElementQuery;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.relations.RelationComparator;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Comparator;
import java.util.List;

/**
 * A vertex-centric query which implements {@link ElementQuery} so that it can be executed by
 * {@link com.thinkaurelius.titan.graphdb.query.QueryProcessor}. Most of the query definition
 * is in the extended {@link BaseVertexCentricQuery} - this class only adds the base vertex to the mix.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexCentricQuery extends BaseVertexCentricQuery implements ElementQuery<TitanRelation, SliceQuery> {

    private final InternalVertex vertex;

    public VertexCentricQuery(InternalVertex vertex, Condition<TitanRelation> condition,
                              Direction direction,
                              List<BackendQueryHolder<SliceQuery>> queries,
                              OrderList orders,
                              int limit) {
        super(condition, direction, queries, orders, limit);
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    public VertexCentricQuery(InternalVertex vertex, BaseVertexCentricQuery base) {
        super(base);
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    /**
     * Constructs an empty query
     * @param vertex
     */
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
        return new RelationComparator(vertex,getOrders());
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
