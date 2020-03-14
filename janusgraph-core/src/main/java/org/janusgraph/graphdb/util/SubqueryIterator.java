/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.graphdb.util;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author davidclement90@laposte.net
 */
public class SubqueryIterator implements Iterator<JanusGraphElement>, AutoCloseable {

    private final Cache<JointIndexQuery.Subquery, List<Object>> indexCache;

    private Iterator<? extends JanusGraphElement> elementIterator;

    private List<Object> currentIds;

    private QueryProfiler streamedQueryProfiler;

    private JointIndexQuery.Subquery streamedQuery;

    private boolean isTimerRunning;

    public SubqueryIterator(List<JointIndexQuery.Subquery> queries, IndexSerializer indexSerializer,
                            BackendTransaction tx, Cache<JointIndexQuery.Subquery, List<Object>> indexCache,
                            final int limit, Function<Object, ? extends JanusGraphElement> conversionFunction) {
        Preconditions.checkArgument(!queries.isEmpty());
        Preconditions.checkArgument(limit >= 0, "Invalid limit: %s", limit);
        this.indexCache = indexCache;
        if (limit == Query.NO_LIMIT || queries.size() == 1) {
            // If there is no limit, we lazily stream the first query and eagerly execute the rest of queries all at once
            // Otherwise if there is only one query, we just lazily stream it
            streamFirstAndIntersectOthers(queries, indexSerializer, tx, limit, conversionFunction);
        } else {
            // For multiple queries, we progressively fetch results and take intersections
            intersectAll(queries, indexSerializer, tx, limit, conversionFunction);
        }
    }

    @Override
    public boolean hasNext() {
        if (!elementIterator.hasNext() && currentIds != null) {
            indexCache.put(streamedQuery, currentIds);
            streamedQueryProfiler.stopTimer();
            isTimerRunning = false;
            streamedQueryProfiler.setResultSize(currentIds.size());
        }
        return elementIterator.hasNext();
    }

    @Override
    public JanusGraphElement next() {
        return elementIterator.next();
    }

    @Override
    public void close() throws Exception {
        if (isTimerRunning) {
            streamedQueryProfiler.stopTimer();
        }
    }

    private void streamFirstAndIntersectOthers(List<JointIndexQuery.Subquery> queries, IndexSerializer indexSerializer,
                                               BackendTransaction tx, final int limit,
                                               Function<Object, ? extends JanusGraphElement> conversionFunction) {
        Stream<?> stream;
        JointIndexQuery.Subquery firstQuery = queries.get(0).updateLimit(limit);
        final List<Object> cacheResponse = indexCache.getIfPresent(firstQuery);
        if (cacheResponse != null) {
            stream = cacheResponse.stream();
        } else {
            currentIds = new ArrayList<>();
            streamedQuery = firstQuery;
            streamedQueryProfiler = QueryProfiler.startProfile(firstQuery.getProfiler(), firstQuery);
            isTimerRunning = true;
            stream = indexSerializer.query(firstQuery, tx).peek(r -> currentIds.add(r));
        }

        // retrieve results from the rest queries and do intersection
        Set<Object> otherResults = null;
        for (int i = 1; i < queries.size(); i++) {
            JointIndexQuery.Subquery subQuery = queries.get(i).updateLimit(Query.NO_LIMIT);
            try {
                Set<Object> subResults = new HashSet<>(indexCache.get(subQuery, () -> {
                    QueryProfiler profiler = subQuery.getProfiler();
                    QueryProfiler.startProfile(profiler, subQuery);
                    List<Object> queryResults = indexSerializer.query(subQuery, tx).collect(Collectors.toList());
                    profiler.stopTimer();
                    profiler.setResultSize(queryResults.size());
                    return queryResults;
                }));
                if (i == 1) {
                    otherResults = subResults;
                } else {
                    assert otherResults != null;
                    otherResults.retainAll(subResults);
                }
            } catch (Exception e) {
                throw new JanusGraphException("Could not execute index query", e.getCause());
            }
        }

        Set<Object> others = otherResults;
        // combine the results of lazily streamed first query and the results of rest queries
        elementIterator = stream.filter(e -> others == null || others.contains(e))
            .map(conversionFunction).map(r -> (JanusGraphElement) r).iterator();
    }

    private void intersectAll(List<JointIndexQuery.Subquery> queries, IndexSerializer indexSerializer,
                              BackendTransaction tx, final int limit,
                              Function<Object, ? extends JanusGraphElement> conversionFunction) {
        final int multiplier = Math.min(128, (int) Math.pow(2, Math.pow(2, queries.size() - 1)));
        int baseSubLimit = Math.min(limit * multiplier, Query.NO_LIMIT);
        // A mapping of result to a number list of queries that contain this result
        Map<Object, List<Integer>> subResultToQueryMap = new LinkedHashMap<>();
        double[] scores = new double[queries.size()];
        int[] offsets = new int[queries.size()];
        boolean[] resultsExhausted = new boolean[queries.size()];
        int resultsExhaustedCount = 0;
        List<Object> results = new ArrayList<>();
        do {
            double scoreSum = 0;
            for (double score : scores) scoreSum += score;
            for (int idx = 0; idx < queries.size(); idx++) {
                // Pick up suitable queries to execute. We pick up queries with score lower than the average.
                if (resultsExhausted[idx] || scores[idx] > scoreSum / (queries.size() - resultsExhaustedCount))
                    continue;
                int subLimit;
                if (offsets[idx] > 0) {
                    // Estimate the best subLimit based on previous round of query. It is a reasonable assumption
                    // that no_of_total_results_needed : optimal_sublimit can be approximated by
                    // no_of_last_intersected_results : last_sublimit.
                    subLimit = results.size() > 0 ? limit * offsets[idx] / results.size() * 2
                        : (int) Math.pow(offsets[idx], 1.5);
                } else {
                    subLimit = baseSubLimit;
                }
                subLimit = Math.min(Query.NO_LIMIT, subLimit);

                executeQueries(queries, idx, subLimit, offsets, subResultToQueryMap, indexSerializer, tx);

                if (offsets[idx] < subLimit) {
                    resultsExhausted[idx] = true;
                    resultsExhaustedCount++;
                }
            }

            intersectResults(subResultToQueryMap, results, limit, queries.size());

            scores = computeScores(resultsExhaustedCount, resultsExhausted, offsets, queries.size(), results.size(),
                limit, subResultToQueryMap);

        } while (resultsExhaustedCount < queries.size() && results.size() < limit);
        elementIterator = results.stream().limit(limit).map(conversionFunction).map(r -> (JanusGraphElement) r).iterator();
    }

    private void executeQueries(List<JointIndexQuery.Subquery> queries, final int idx, final int subLimit,
                                int[] offsets, Map<Object, List<Integer>> subResultToQueryMap,
                                IndexSerializer indexSerializer, BackendTransaction tx) {
        JointIndexQuery.Subquery subQuery = queries.get(idx).updateLimit(subLimit);
        final List<Object> subQueryCache = indexCache.getIfPresent(subQuery);
        if (subQueryCache != null) {
            assert subQueryCache.size() >= offsets[idx];
            for (int j = offsets[idx]; j < subQueryCache.size(); j++) {
                subResultToQueryMap.computeIfAbsent(subQueryCache.get(j), k -> new ArrayList<>()).add(idx);
            }
            offsets[idx] = subQueryCache.size();
        } else {
            QueryProfiler profiler = subQuery.getProfiler();
            QueryProfiler.startProfile(profiler, subQuery);
            // TODO: leverage the scrolling capability of indexing backends ratherthan throw away old results
            indexSerializer.query(subQuery, tx).skip(offsets[idx]).forEachOrdered(result -> {
                offsets[idx]++;
                subResultToQueryMap.computeIfAbsent(result, k -> new ArrayList<>()).add(idx);
            });
            profiler.stopTimer();
            profiler.setResultSize(offsets[idx]);
            // TODO: should we put it into cache?
        }

    }

    private void intersectResults(Map<Object, List<Integer>> subResultToQueryMap,
                                  List<Object> results, final int limit, final int numberOfQueries) {
        for (Iterator<Map.Entry<Object, List<Integer>>> it = subResultToQueryMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Object, List<Integer>> entry = it.next();
            if (entry.getValue().size() == numberOfQueries) {
                // this particular result satisfies every index query
                results.add(entry.getKey());
                if (results.size() >= limit) break;
                it.remove();
            }
        }
    }

    /**
     * Compute score for each query. Lower score means the query is more selective and more likely to
     * be the bottleneck. Exhausted queries always have 0 score. Unless more factors are taken into consideration,
     * at the moment it does not make sense to compare queries if we only have two.
     */
    private double[] computeScores(final int resultsExhaustedCount, boolean[] resultsExhausted,
                                   int[] offsets, final int numberOfQueries, final int numberOfResults,
                                   final int limit, Map<Object, List<Integer>> subResultToQueryMap) {
        double[] scores = new double[numberOfQueries];
        // TODO: take more factors into consideration, e.g. query latency
        if (resultsExhaustedCount < numberOfQueries && numberOfResults < limit && numberOfQueries > 2) {
            // A query whose results have many intersections with other queries is less likely to be selective.
            // A query with few intersections with other queries is more valuable in the sense that it contains
            // less redundant information, and thus is more useful.
            // In other words, if multiple index queries have huge overlaps with each other except one query,
            // then that query is more likely to be the bottleneck.
            for (List<Integer> queryNoList : subResultToQueryMap.values()) {
                for (int idx : queryNoList) {
                    if (!resultsExhausted[idx]) scores[idx] += Math.log(queryNoList.size());
                }
            }
            // prevent the number of executions per query from diverging too much
            for (int i = 0; i < scores.length; i++) scores[i] /= Math.log(offsets[i]);
        }
        return scores;
    }

}
