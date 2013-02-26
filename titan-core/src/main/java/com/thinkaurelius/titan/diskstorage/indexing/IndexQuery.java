package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import org.apache.commons.lang.StringUtils;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class IndexQuery {

    private final KeyCondition<String> condition;
    private final String store;

    public IndexQuery(String store, KeyCondition<String> condition) {
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(StringUtils.isNotBlank(store));
        this.condition = condition;
        this.store=store;
    }

    public KeyCondition<String> getCondition() {
        return condition;
    }

    public String getStore() {
        return store;
    }

    @Override
    public int hashCode() {
        return condition.hashCode()*9876469 + store.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        IndexQuery oth = (IndexQuery)other;
        return store.equals(oth.store) && condition.equals(oth.condition);
    }

    @Override
    public String toString() {
        return "["+condition.toString()+"]:"+store;
    }

}
