package com.thinkaurelius.titan.graphdb.olap;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.vertex.BaseVertexCentricQuery;
import com.thinkaurelius.titan.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class QueryContainer {

    public static final int DEFAULT_HARD_QUERY_LIMIT = 100000;
    public static final String QUERY_NAME_PREFIX = "query$";

    private final StandardTitanTx tx;
    private int hardQueryLimit;
    private final boolean requiresName;

    private Set<Query> queries;
    private SetMultimap<SliceQuery, Query> inverseQueries;

    public QueryContainer(StandardTitanTx tx) {
        Preconditions.checkArgument(tx != null);
        this.tx = tx;
        queries = new HashSet<>(6);
        inverseQueries = HashMultimap.create();
        hardQueryLimit = DEFAULT_HARD_QUERY_LIMIT;
        requiresName = true;
    }

    public TitanTransaction getTransaction() {
        return tx;
    }

    public QueryBuilder addQuery() {
        return new QueryBuilder();
    }

//    Query getQuery(String name) {
//        return queries.get(name);
//    }

    Set<Query> getQueries(SliceQuery slice) {
        return inverseQueries.get(slice);
    }

    Iterable<Query> getQueries() {
        return queries;
    }

    public List<SliceQuery> getSliceQueries() {
        List<SliceQuery> slices = new ArrayList<>(queries.size() * 2);
        for (QueryContainer.Query q : getQueries()) {
            for (SliceQuery slice : q.getSlices()) {
                if (!slices.contains(slice)) slices.add(slice);
            }
        }
        return slices;
    }

    static class Query {

        private final List<SliceQuery> slices;
        //        private final String name;
        private final RelationCategory returnType;

        public Query(List<SliceQuery> slices, RelationCategory returnType) {
            this.slices = slices;
//            this.name = name;
            this.returnType = returnType;
        }

        public List<SliceQuery> getSlices() {
            return slices;
        }

//        public String getName() {
//            return name;
//        }

        public RelationCategory getReturnType() {
            return returnType;
        }
    }

    public class QueryBuilder extends BasicVertexCentricQueryBuilder<QueryBuilder> {

//        private String name = null;

        private QueryBuilder() {
            super(QueryContainer.this.tx);
        }

        private Query relations(RelationCategory returnType) {
//            if (name==null) {
//                if (hasSingleType()) name = getSingleType().name();
//                else if (!requiresName) name = QUERY_NAME_PREFIX + queries.size();
//                else throw new IllegalStateException("Need to specify an explicit name for this query");
//            }

            BaseVertexCentricQuery vq = super.constructQuery(returnType);
            List<SliceQuery> slices = new ArrayList<>(vq.numSubQueries());
            for (int i = 0; i < vq.numSubQueries(); i++) {
                BackendQueryHolder<SliceQuery> bq = vq.getSubQuery(i);
                SliceQuery sq = bq.getBackendQuery();
                slices.add(sq.updateLimit(bq.isFitted() ? vq.getLimit() : hardQueryLimit));
            }
            Query q = new Query(slices, returnType);
            synchronized (queries) {
                Preconditions.checkArgument(!queries.contains(q), "Query has already been added: %s", q);
                queries.add(q);
                for (SliceQuery sq : slices) {
                    inverseQueries.put(sq, q);
                }
            }
            return q;

        }

        @Override
        protected QueryBuilder getThis() {
            return this;
        }

//        /**
//         * Sets the name for this query
//         * @param name
//         * @return
//         */
//        public QueryBuilder setName(String name) {
//            Preconditions.checkArgument(StringUtils.isNotBlank(name), "Invalid name provided: %s", name);
//            this.name=name;
//            return getThis();
//        }

        public void edges() {
            relations(RelationCategory.EDGE);
        }

        public void relations() {
            relations(RelationCategory.RELATION);
        }

        public void properties() {
            relations(RelationCategory.PROPERTY);
        }

        /*
        ########### SIMPLE OVERWRITES ##########
         */

        @Override
        public QueryBuilder has(String type, Object value) {
            super.has(type, value);
            return this;
        }

        @Override
        public QueryBuilder hasNot(String key, Object value) {
            super.hasNot(key, value);
            return this;
        }

        @Override
        public QueryBuilder has(String key) {
            super.has(key);
            return this;
        }

        @Override
        public QueryBuilder hasNot(String key) {
            super.hasNot(key);
            return this;
        }

        @Override
        public QueryBuilder has(String key, TitanPredicate predicate, Object value) {
            super.has(key, predicate, value);
            return this;
        }

        @Override
        public <T extends Comparable<?>> QueryBuilder interval(String key, T start, T end) {
            super.interval(key, start, end);
            return this;
        }

        @Override
        public QueryBuilder types(RelationType... types) {
            super.types(types);
            return this;
        }

        @Override
        public QueryBuilder labels(String... labels) {
            super.labels(labels);
            return this;
        }

        @Override
        public QueryBuilder keys(String... keys) {
            super.keys(keys);
            return this;
        }

        public QueryBuilder type(RelationType type) {
            super.type(type);
            return this;
        }

        @Override
        public QueryBuilder direction(Direction d) {
            super.direction(d);
            return this;
        }

        @Override
        public QueryBuilder limit(int limit) {
            super.limit(limit);
            return this;
        }

        @Override
        public QueryBuilder orderBy(String key, Order order) {
            super.orderBy(key, order);
            return this;
        }


    }


}
