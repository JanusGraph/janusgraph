package com.thinkaurelius.titan.graphdb.query.profile;

import com.thinkaurelius.titan.graphdb.query.Query;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface QueryProfiler {

    public static final String CONDITION_ANNOTATION = "condition";
    public static final String ORDERS_ANNOTATION = "orders";
    public static final String LIMIT_ANNOTATION = "limit";

    public static final String MULTIQUERY_ANNOTATION = "multi";
    public static final String NUMVERTICES_ANNOTATION = "vertices";
    public static final String PARTITIONED_VERTEX_ANNOTATION = "partitioned";

    public static final String FITTED_ANNOTATION = "isFitted";
    public static final String ORDERED_ANNOTATION = "isOrdered";
    public static final String QUERY_ANNOTATION = "query";
    public static final String FULLSCAN_ANNOTATION = "fullscan";
    public static final String INDEX_ANNOTATION = "index";

    public static final String OR_QUERY = "OR-query";
    public static final String AND_QUERY = "AND-query";
    public static final String OPTIMIZATION = "optimization";

    public static final QueryProfiler NO_OP = new QueryProfiler() {
        @Override
        public QueryProfiler addNested(String groupName) {
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


    public QueryProfiler addNested(String groupName);

    public QueryProfiler setAnnotation(String key, Object value);

    public void startTimer();

    public void stopTimer();

    public void setResultSize(long size);

    public static<Q extends Query,R extends Collection> R profile(QueryProfiler profiler, Q query, Function<Q,R> queryExecutor) {
        return profile(profiler,query,false,queryExecutor);
    }

    public static<Q extends Query,R extends Collection> R profile(String groupName, QueryProfiler profiler, Q query, Function<Q,R> queryExecutor) {
        return profile(groupName,profiler,query,false,queryExecutor);
    }

    public static<Q extends Query,R extends Collection> R profile(QueryProfiler profiler, Q query, boolean multiQuery, Function<Q,R> queryExecutor) {
        return profile("backend-query",profiler,query,multiQuery,queryExecutor);
    }

    public static<Q extends Query,R extends Collection> R profile(String groupName, QueryProfiler profiler, Q query, boolean multiQuery, Function<Q,R> queryExecutor) {
        QueryProfiler sub = profiler.addNested(groupName);
        sub.setAnnotation(QUERY_ANNOTATION, query);
        if (query.hasLimit()) sub.setAnnotation(LIMIT_ANNOTATION,query.getLimit());
        sub.startTimer();
        R result = queryExecutor.apply(query);
        sub.stopTimer();
        long resultSize = 0;
        if (multiQuery && profiler!=QueryProfiler.NO_OP) {
            //The result set is a collection of collections, but don't do this computation if profiling is disabled
            for (Object r : result) {
                if (r instanceof Collection) resultSize+=((Collection)r).size();
                else resultSize++;
            }
        } else {
            resultSize = result.size();
        }
        sub.setResultSize(resultSize);
        return result;
    }

}
