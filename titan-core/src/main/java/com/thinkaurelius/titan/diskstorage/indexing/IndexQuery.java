package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.BackendQuery;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import org.apache.commons.lang.StringUtils;

/**
 * An external index query executed on an {@link IndexProvider}.
 *
 * A query is comprised of the store identifier against which the query ought to be executed and a query condition
 * which defines which entries match the query.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexQuery extends BaseQuery implements BackendQuery<IndexQuery> {

    private final Condition condition;
    private final String store;

    public IndexQuery(String store, Condition condition) {
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition));
        this.condition=condition;
        this.store = store;
    }

    public Condition getCondition() {
        return condition;
    }

    public String getStore() {
        return store;
    }

    public boolean hasStore() {
        return StringUtils.isNotBlank(store);
    }

    @Override
    public IndexQuery setLimit(int limit) {
        Preconditions.checkArgument(!hasLimit());
        super.setLimit(limit);
        return this;
    }

    @Override
    public IndexQuery updateLimit(int newLimit) {
        return new IndexQuery(store,condition).setLimit(newLimit);
    }

    @Override
    public int hashCode() {
        return condition.hashCode()*9876469 + store.hashCode()*4711 + getLimit();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        IndexQuery oth = (IndexQuery)other;
        return ((store ==oth.store) || (store !=null && store.equals(oth.store)))
                && condition.equals(oth.condition) && getLimit()==oth.getLimit();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (hasLimit()) b.append("(").append(getLimit()).append(")");
        b.append(":").append(store);
        return b.toString();
    }

}
