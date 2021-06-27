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

package org.janusgraph.graphdb.query.profile;

import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.graph.JointIndexQuery.Subquery;

import java.util.Collection;
import java.util.function.Function;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface QueryProfiler {

    String CONDITION_ANNOTATION = "condition";
    String ORDERS_ANNOTATION = "orders";
    String LIMIT_ANNOTATION = "limit";

    String MULTIQUERY_ANNOTATION = "multi";
    String MULTIPREFETCH_ANNOTATION = "multiPreFetch";
    String NUMVERTICES_ANNOTATION = "vertices";
    String PARTITIONED_VERTEX_ANNOTATION = "partitioned";

    String FITTED_ANNOTATION = "isFitted";
    String ORDERED_ANNOTATION = "isOrdered";
    String QUERY_ANNOTATION = "query";
    String FULLSCAN_ANNOTATION = "fullscan";
    String INDEX_ANNOTATION = "index";

    /* ==================================================================================
                                       GROUP NAMES
     ==================================================================================*/
    String OR_QUERY = "OR-query";
    String AND_QUERY = "AND-query";
    String BACKEND_QUERY = "backend-query";
    String OPTIMIZATION = "optimization";
    // graph centric query construction phase
    String CONSTRUCT_GRAPH_CENTRIC_QUERY = "constructGraphCentricQuery";
    // graph centric query execution phase
    String GRAPH_CENTRIC_QUERY = "GraphCentricQuery";
    String MIXED_INEX_COUNT_QUERY = "MixedIndexCountQuery";

    QueryProfiler NO_OP = new QueryProfiler() {
        @Override
        public QueryProfiler addNested(String groupName, boolean hasSiblings) {
            return this;
        }

        @Override
        public QueryProfiler setAnnotation(String key, Object value) {
            return this;
        }

        @Override
        public void startTimer() {
        }

        @Override
        public void stopTimer() {
        }

        @Override
        public void setResultSize(long size) {
        }
    };

    default QueryProfiler addNested(String groupName) {
        return addNested(groupName, false);
    }

    QueryProfiler addNested(String groupName, boolean hasSiblings);

    QueryProfiler setAnnotation(String key, Object value);

    void startTimer();

    void stopTimer();

    void setResultSize(long size);

    static<Q extends Query,R extends Collection> R profile(QueryProfiler profiler, Q query, Function<Q,R> queryExecutor) {
        return profile(profiler,query,false,queryExecutor);
    }

    static<Q extends Query,R extends Collection> R profile(QueryProfiler profiler, Q query, boolean multiQuery, Function<Q,R> queryExecutor) {
        return profile(BACKEND_QUERY,profiler,query,multiQuery,queryExecutor);
    }

    static<Q extends Query,R extends Collection> R profile(String groupName, QueryProfiler profiler, Q query, boolean multiQuery, Function<Q,R> queryExecutor) {
        final QueryProfiler sub = profiler.addNested(groupName);
        sub.setAnnotation(QUERY_ANNOTATION, query);
        if (query.hasLimit()) sub.setAnnotation(LIMIT_ANNOTATION,query.getLimit());
        sub.startTimer();
        final R result = queryExecutor.apply(query);
        sub.stopTimer();
        long resultSize = 0;
        if (multiQuery && profiler!=QueryProfiler.NO_OP) {
            //The result set is a collection of collections, but don't do this computation if profiling is disabled
            for (final Object r : result) {
                if (r instanceof Collection) resultSize+=((Collection)r).size();
                else resultSize++;
            }
        } else {
            resultSize = result.size();
        }
        sub.setResultSize(resultSize);
        return result;
    }

    static QueryProfiler startProfile(QueryProfiler profiler, Subquery query) {
        final QueryProfiler sub = profiler.addNested(BACKEND_QUERY);
        sub.setAnnotation(QUERY_ANNOTATION, query);
        if (query.hasLimit()) sub.setAnnotation(LIMIT_ANNOTATION,query.getLimit());
        sub.startTimer();
        return sub;
    }
}
