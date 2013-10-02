package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.query.BackendQuery;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;

/**
 * An external index query executed on an {@link IndexProvider}.
 * <p/>
 * A query is comprised of the store identifier against which the query ought to be executed and a query condition
 * which defines which entries match the query.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexQuery extends BaseQuery implements BackendQuery<IndexQuery> {

    private final Condition condition;
    private final ElementType resultType;

    public IndexQuery(ElementType resultType, Condition condition) {
        Preconditions.checkNotNull(resultType);
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition));
        this.condition = condition;
        this.resultType = resultType;
    }

    public IndexQuery(String resultType, Condition condition) {
        this(ElementType.getByName(resultType), condition);
    }

    public Condition<TitanElement> getCondition() {
        return condition;
    }

    public ElementType getResultType() {
        return resultType;
    }

    public String getStore() {
        return resultType.toString();
    }


    @Override
    public IndexQuery setLimit(int limit) {
        Preconditions.checkArgument(!hasLimit());
        super.setLimit(limit);
        return this;
    }

    @Override
    public IndexQuery updateLimit(int newLimit) {
        return new IndexQuery(resultType, condition).setLimit(newLimit);
    }

    @Override
    public int hashCode() {
        return condition.hashCode() * 9876469 + resultType.hashCode() * 4711 + getLimit();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        IndexQuery oth = (IndexQuery) other;
        return (resultType == oth.resultType)
                && condition.equals(oth.condition) && getLimit() == oth.getLimit();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (hasLimit()) b.append("(").append(getLimit()).append(")");
        b.append(":").append(resultType);
        return b.toString();
    }

}
