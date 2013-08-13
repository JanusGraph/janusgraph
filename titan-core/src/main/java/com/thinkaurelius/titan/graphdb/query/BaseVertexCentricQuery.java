package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.FixedCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

class BaseVertexCentricQuery extends BaseQuery {

    private final Condition<TitanRelation> condition;
    private final List<BackendQueryHolder<SliceQuery>> queries;

    public BaseVertexCentricQuery(Condition<TitanRelation> condition,
                                  List<BackendQueryHolder<SliceQuery>> queries,
                                  int limit) {
        super(limit);
        Preconditions.checkNotNull(condition);
        Preconditions.checkNotNull(queries);
        Preconditions.checkArgument(limit >= 0);
        this.condition = condition;
        this.queries = queries;
    }

    protected BaseVertexCentricQuery(BaseVertexCentricQuery query) {
        super(query.getLimit());
        this.condition=query.condition;
        this.queries=query.queries;
    }

    protected BaseVertexCentricQuery() {
        this(new FixedCondition<TitanRelation>(false), new ArrayList<BackendQueryHolder<SliceQuery>>(0),0);
    }

    public static final BaseVertexCentricQuery emptyQuery() {
        return new BaseVertexCentricQuery();
    }

    public Condition<TitanRelation> getCondition() {
        return condition;
    }

    protected List<BackendQueryHolder<SliceQuery>> getQueries() {
        return queries;
    }

    public boolean isEmpty() {
        return getLimit()<=0;
    }

    @Override
    public BaseVertexCentricQuery setLimit(int limit) {
        throw new UnsupportedOperationException();
    }

    public int numSubQueries() {
        return queries.size();
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

}
