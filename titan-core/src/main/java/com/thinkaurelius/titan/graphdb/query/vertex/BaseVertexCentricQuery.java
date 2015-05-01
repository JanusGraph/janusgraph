package com.thinkaurelius.titan.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.FixedCondition;
import com.thinkaurelius.titan.graphdb.query.profile.ProfileObservable;
import com.thinkaurelius.titan.graphdb.query.profile.QueryProfiler;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.List;

/**
 * The base implementation for {@link VertexCentricQuery} which does not yet contain a reference to the
 * base vertex of the query. This query is constructed by {@link BasicVertexCentricQueryBuilder#constructQuery(com.thinkaurelius.titan.graphdb.internal.RelationCategory)}
 * and then later extended by single or multi-vertex query which add the vertex to the query.
 * </p>
 * This class override many methods in {@link com.thinkaurelius.titan.graphdb.query.ElementQuery} - check there
 * for a description.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BaseVertexCentricQuery extends BaseQuery implements ProfileObservable {

    /**
     * The condition of this query in QNF
     */
    protected final Condition<TitanRelation> condition;
    /**
     * The individual component {@link SliceQuery} of this query. This query is considered an OR
     * of the individual components (possibly filtered by the condition if not fitted).
     */
    protected final List<BackendQueryHolder<SliceQuery>> queries;
    /**
     * The result order of this query (if any)
     */
    private final OrderList orders;
    /**
     * The direction condition of this query. This is duplicated from the condition for efficiency reasons.
     */
    protected final Direction direction;

    public BaseVertexCentricQuery(Condition<TitanRelation> condition, Direction direction,
                                  List<BackendQueryHolder<SliceQuery>> queries, OrderList orders,
                                  int limit) {
        super(limit);
        Preconditions.checkArgument(condition != null && queries != null && direction != null);
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition) && limit>=0);
        this.condition = condition;
        this.queries = queries;
        this.orders = orders;
        this.direction=direction;
    }

    protected BaseVertexCentricQuery(BaseVertexCentricQuery query) {
        this(query.getCondition(), query.getDirection(), query.getQueries(), query.getOrders(), query.getLimit());
    }

    /**
     * Construct an empty query
     */
    protected BaseVertexCentricQuery() {
        this(new FixedCondition<TitanRelation>(false), Direction.BOTH, new ArrayList<BackendQueryHolder<SliceQuery>>(0),OrderList.NO_ORDER,0);
    }

    public static BaseVertexCentricQuery emptyQuery() {
        return new BaseVertexCentricQuery();
    }

    public Condition<TitanRelation> getCondition() {
        return condition;
    }

    public OrderList getOrders() {
        return orders;
    }

    public Direction getDirection() {
        return direction;
    }

    protected List<BackendQueryHolder<SliceQuery>> getQueries() {
        return queries;
    }

    public boolean isEmpty() {
        return getLimit()<=0;
    }

    public int numSubQueries() {
        return queries.size();
    }

    /**
     * A query is considered 'simple' if it is comprised of just one sub-query and that query
     * is fitted (i.e. does not require an in-memory filtering).
     * @return
     */
    public boolean isSimple() {
        return queries.size()==1 && queries.get(0).isFitted() && queries.get(0).isSorted();
    }

    public BackendQueryHolder<SliceQuery> getSubQuery(int position) {
        return queries.get(position);
    }

    public boolean matches(TitanRelation relation) {
        return condition.evaluate(relation);
    }

    @Override
    public String toString() {
        String s = "["+condition.toString()+"]";
        if (hasLimit()) s+=":"+getLimit();
        return s;
    }

    @Override
    public void observeWith(QueryProfiler profiler) {
        profiler.setAnnotation(QueryProfiler.CONDITION_ANNOTATION,condition);
        profiler.setAnnotation(QueryProfiler.ORDERS_ANNOTATION,orders);
        if (hasLimit()) profiler.setAnnotation(QueryProfiler.LIMIT_ANNOTATION,getLimit());
        queries.forEach(bqh -> bqh.observeWith(profiler));
    }
}
