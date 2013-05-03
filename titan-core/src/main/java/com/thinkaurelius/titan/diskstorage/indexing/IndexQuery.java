package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import org.apache.commons.lang.StringUtils;

/**
 * An external index query executed on an {@link IndexProvider}.
 *
 * A query is comprised of the store identifier against which the query ought to be executed and a query condition
 * which defines which entries match the query.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexQuery {

    private final KeyCondition<String> condition;
    private final String store;
    private final int limit;


    public IndexQuery(String store, KeyCondition<String> condition) {
        this(store,condition,Query.NO_LIMIT);
    }

    public IndexQuery(String store, KeyCondition<String> condition, int limit) {
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(StringUtils.isNotBlank(store));
        Preconditions.checkArgument(limit>=0);
        this.condition=condition;
        this.store=store;
        this.limit=limit;
    }

    public KeyCondition<String> getCondition() {
        return condition;
    }

    public String getStore() {
        return store;
    }

    /**
     *
     * @return The maximum number of results to return
     */
    public int getLimit() {
        return limit;
    }

    public boolean hasLimit() {
        return limit!= Query.NO_LIMIT;
    }

    @Override
    public int hashCode() {
        return condition.hashCode()*9876469 + store.hashCode()*4711 + limit;
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        IndexQuery oth = (IndexQuery)other;
        return store.equals(oth.store) && condition.equals(oth.condition) && limit==oth.limit;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (hasLimit()) b.append("(").append(limit).append(")");
        b.append(":").append(store);
        return b.toString();
    }

}
