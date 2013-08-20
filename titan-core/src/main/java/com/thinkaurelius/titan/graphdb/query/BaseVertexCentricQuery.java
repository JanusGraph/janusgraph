package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.FixedCondition;
import com.tinkerpop.blueprints.Direction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

class BaseVertexCentricQuery extends BaseQuery {

    //Condition in CNF
    private final Condition<TitanRelation> condition;
    private final List<BackendQueryHolder<SliceQuery>> queries;
    private final Direction direction;

    public BaseVertexCentricQuery(Condition<TitanRelation> condition, Direction direction,
                                  List<BackendQueryHolder<SliceQuery>> queries,
                                  int limit) {
        super(limit);
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition));
        Preconditions.checkNotNull(queries);
        Preconditions.checkNotNull(direction);
        Preconditions.checkArgument(limit >= 0);
        this.condition = condition;
        this.queries = queries;
        this.direction=direction;
    }

    protected BaseVertexCentricQuery(BaseVertexCentricQuery query) {
        super(query.getLimit());
        this.condition=query.condition;
        this.queries=query.queries;
        this.direction=query.direction;
    }

    protected BaseVertexCentricQuery() {
        this(new FixedCondition<TitanRelation>(false),Direction.BOTH, new ArrayList<BackendQueryHolder<SliceQuery>>(0),0);
    }

    public static final BaseVertexCentricQuery emptyQuery() {
        return new BaseVertexCentricQuery();
    }

    public Condition<TitanRelation> getCondition() {
        return condition;
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
