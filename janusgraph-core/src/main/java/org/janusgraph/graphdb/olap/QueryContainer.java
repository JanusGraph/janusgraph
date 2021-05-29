// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.olap;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.RelationType;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.query.BackendQueryHolder;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.vertex.BaseVertexCentricQuery;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

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

    private final StandardJanusGraphTx tx;
    private final int hardQueryLimit;

    private final Set<Query> queries;
    private final SetMultimap<SliceQuery, Query> inverseQueries;

    public QueryContainer(StandardJanusGraphTx tx) {
        this.tx = Preconditions.checkNotNull(tx);
        queries = new HashSet<>(6);
        inverseQueries = HashMultimap.create();
        hardQueryLimit = DEFAULT_HARD_QUERY_LIMIT;
    }

    public JanusGraphTransaction getTransaction() {
        return tx;
    }

    public QueryBuilder addQuery() {
        return new QueryBuilder();
    }


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
        public QueryBuilder has(String key, JanusGraphPredicate predicate, Object value) {
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
