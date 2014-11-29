package com.thinkaurelius.titan.graphdb.query.graph;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.query.BackendQuery;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.types.CompositeIndexType;
import com.thinkaurelius.titan.graphdb.types.MixedIndexType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * A component/sub-query of a {@link GraphCentricQuery} that gets executed against an indexing backend or the index store
 * by the query processor of the enclosing transaction.
 * </p>
 * This query itself can contain multiple sub-queries which are individually executed by the {@link com.thinkaurelius.titan.graphdb.database.IndexSerializer}
 * and the result sets merged.
 * </p>
 * Those sub-queries are either targeting an external indexing backend or the internal index store which is a distinction this
 * query keeps track of through the sub-class {@link Subquery}, since their definition and execution differs starkly.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JointIndexQuery extends BaseQuery implements BackendQuery<JointIndexQuery> {

    private final List<Subquery> queries;

    private JointIndexQuery(List<Subquery> queries) {
        Preconditions.checkArgument(queries!=null);
        this.queries = queries;
    }

    public JointIndexQuery() {
        this.queries = new ArrayList<Subquery>(4);
    }

    public void add(MixedIndexType index, IndexQuery query) {
        queries.add(new Subquery(index, query));
    }

    public void add(CompositeIndexType index, MultiKeySliceQuery query) {
        queries.add(new Subquery(index, query));
    }

    public int size() {
        return queries.size();
    }

    public Subquery getQuery(int pos) {
        return queries.get(pos);
    }

    public boolean isEmpty() {
        return queries.isEmpty();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(queries).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        JointIndexQuery oth = (JointIndexQuery) other;
        return oth.queries.equals(queries);
    }

    @Override
    public String toString() {
        return queries.toString();
    }

    @Override
    public JointIndexQuery updateLimit(int newLimit) {
        JointIndexQuery ji = new JointIndexQuery(Lists.newArrayList(queries));
        ji.setLimit(newLimit);
        return ji;
    }

    public static class Subquery implements BackendQuery<Subquery> {

        private final IndexType index;
        private final BackendQuery query;

        private Subquery(IndexType index, BackendQuery query) {
            assert index!=null && query!=null && (query instanceof MultiKeySliceQuery || query instanceof IndexQuery);
            this.index = index;
            this.query = query;
        }

        public IndexType getIndex() {
            return index;
        }

        public IndexQuery getMixedQuery() {
            Preconditions.checkArgument(index.isMixedIndex() && query instanceof IndexQuery);
            return (IndexQuery)query;
        }

        public MultiKeySliceQuery getCompositeQuery() {
            Preconditions.checkArgument(index.isCompositeIndex() && query instanceof MultiKeySliceQuery);
            return (MultiKeySliceQuery)query;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(index).append(query).toHashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            else if (other == null) return false;
            else if (!getClass().isInstance(other)) return false;
            Subquery oth = (Subquery) other;
            return index.equals(oth.index) && query.equals(oth.query);
        }

        @Override
        public String toString() {
            return index.toString()+":"+query.toString();
        }

        @Override
        public Subquery updateLimit(int newLimit) {
            return new Subquery(index,query.updateLimit(newLimit));
        }

        @Override
        public boolean hasLimit() {
            return query.hasLimit();
        }

        @Override
        public int getLimit() {
            return query.getLimit();
        }
    }

}
