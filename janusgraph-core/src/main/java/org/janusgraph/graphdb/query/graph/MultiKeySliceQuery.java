package org.janusgraph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.graphdb.query.BackendQuery;
import org.janusgraph.graphdb.query.BaseQuery;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MultiKeySliceQuery extends BaseQuery implements BackendQuery<MultiKeySliceQuery>  {

    private final List<KeySliceQuery> queries;

    public MultiKeySliceQuery(List<KeySliceQuery> queries) {
        Preconditions.checkArgument(queries!=null && !queries.isEmpty());
        this.queries = queries;
    }

    @Override
    public MultiKeySliceQuery updateLimit(int newLimit) {
        MultiKeySliceQuery newQuery = new MultiKeySliceQuery(queries);
        newQuery.setLimit(newLimit);
        return newQuery;
    }

    public List<EntryList> execute(final BackendTransaction tx) {
        int total = 0;
        List<EntryList> result = new ArrayList<EntryList>(4);
        for (KeySliceQuery ksq : queries) {
            EntryList next =tx.indexQuery(ksq.updateLimit(getLimit()-total));
            result.add(next);
            total+=next.size();
            if (total>=getLimit()) break;
        }
        return result;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(queries).append(getLimit()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        MultiKeySliceQuery oth = (MultiKeySliceQuery) other;
        return getLimit()==oth.getLimit() && queries.equals(oth.queries);
    }

    @Override
    public String toString() {
        return "multiKSQ["+queries.size()+"]@"+getLimit();
    }

}
