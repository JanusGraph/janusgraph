package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class JointIndexQuery extends BaseQuery implements BackendQuery<JointIndexQuery> {

    private final List<String> indexes;
    private final List<IndexQuery> queries;

    private JointIndexQuery(List<String> indexes, List<IndexQuery> queries) {
        Preconditions.checkNotNull(indexes);
        Preconditions.checkNotNull(queries);
        this.indexes = indexes;
        this.queries = queries;
    }


    public JointIndexQuery() {
        this.queries = new ArrayList<IndexQuery>(4);
        this.indexes = new ArrayList<String>(4);
    }

    public void add(String index, IndexQuery query) {
        indexes.add(index);
        queries.add(query);
    }

    public int size() {
        return indexes.size();
    }

    public IndexQuery getQuery(int pos) {
        return queries.get(pos);
    }

    public String getIndex(int pos) {
        return indexes.get(pos);
    }

    public boolean isEmpty() {
        return queries.isEmpty();
    }

    @Override
    public int hashCode() {
        return queries.hashCode() * 330401 + indexes.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        JointIndexQuery oth = (JointIndexQuery) other;
        return oth.queries.equals(queries) && oth.indexes.equals(indexes);
    }

    @Override
    public String toString() {
        return queries.toString();
    }

    @Override
    public JointIndexQuery updateLimit(int newLimit) {
        JointIndexQuery ji = new JointIndexQuery(Lists.newArrayList(indexes), Lists.newArrayList(queries));
        ji.setLimit(newLimit);
        return ji;
    }
}
