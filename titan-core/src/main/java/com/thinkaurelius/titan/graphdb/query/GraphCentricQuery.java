package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.FixedCondition;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Comparator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class GraphCentricQuery extends BaseQuery implements ElementQuery<TitanElement, JointIndexQuery> {

    private final Condition<TitanElement> condition;
    private final BackendQueryHolder<JointIndexQuery> indexQuery;
    private final OrderList orders;
    private final ElementType resultType;

    public GraphCentricQuery(ElementType resultType, Condition<TitanElement> condition, OrderList orders,
                             BackendQueryHolder<JointIndexQuery> indexQuery, int limit) {
        super(limit);
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(orders != null && orders.isImmutable());
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition));
        Preconditions.checkNotNull(resultType);
        Preconditions.checkNotNull(indexQuery);
        this.condition = condition;
        this.orders = orders;
        this.resultType = resultType;
        this.indexQuery = indexQuery;
    }

    public static final GraphCentricQuery emptyQuery(ElementType resultType) {
        Condition<TitanElement> cond = new FixedCondition<TitanElement>(false);
        return new GraphCentricQuery(resultType, cond, OrderList.NO_ORDER,
                new BackendQueryHolder<JointIndexQuery>(new JointIndexQuery(),
                        true, false, null), 0);
    }

    public Condition<TitanElement> getCondition() {
        return condition;
    }

    public ElementType getResultType() {
        return resultType;
    }

    public OrderList getOrder() {
        return orders;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (!orders.isEmpty()) b.append(getLimit());
        if (hasLimit()) b.append("(").append(getLimit()).append(")");
        b.append(":").append(resultType.toString());
        return b.toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(condition).append(resultType).append(orders).append(getLimit()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        GraphCentricQuery oth = (GraphCentricQuery) other;
        return resultType == oth.resultType && condition.equals(oth.condition) &&
                orders.equals(oth.getOrder()) && getLimit() == oth.getLimit();
    }

    @Override
    public boolean isEmpty() {
        return getLimit() <= 0;
    }

    @Override
    public int numSubQueries() {
        return 1;
    }

    @Override
    public BackendQueryHolder<JointIndexQuery> getSubQuery(int position) {
        if (position == 0) return indexQuery;
        else throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean isSorted() {
        return !orders.isEmpty();
    }

    @Override
    public Comparator<TitanElement> getSortOrder() {
        if (orders.isEmpty()) return new ComparableComparator();
        else return orders;
    }

    @Override
    public boolean hasDuplicateResults() {
        return false;
    }

    @Override
    public boolean matches(TitanElement element) {
        return condition.evaluate(element);
    }


}
