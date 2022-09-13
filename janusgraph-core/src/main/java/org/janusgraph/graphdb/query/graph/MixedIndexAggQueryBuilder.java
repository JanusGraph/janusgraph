// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import org.janusgraph.core.MixedIndexAggQuery;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;

/**
 * Builds a {@link MixedIndexAggQuery}, which contains a single query against a mixed index. It is used to retrieve
 * aggregated result of elements satisfying given conditions.It is used to retrieve.
 *
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class MixedIndexAggQueryBuilder implements MixedIndexAggQuery {
    private final BackendTransaction txHandle;
    private final IndexSerializer serializer;
    /**
     * Search query against index backend
     */
    private JointIndexQuery.Subquery query;
    /**
     * The profiler observing this query
     */
    private QueryProfiler profiler = QueryProfiler.NO_OP;

    public MixedIndexAggQueryBuilder(IndexSerializer serializer, BackendTransaction txHandle) {
        Preconditions.checkNotNull(serializer);
        this.serializer = serializer;
        this.txHandle = txHandle;
    }

    public MixedIndexAggQueryBuilder constructIndex(JointIndexQuery indexQuery, ElementCategory resultType) {
        if (indexQuery.size() != 1 || !indexQuery.getQuery(0).getIndex().isMixedIndex()) {
            return null;
        }
        JointIndexQuery.Subquery subquery = indexQuery.getQuery(0);
        this.query = subquery;
        return this;
    }

    @Override
    public Number execute(Aggregation aggregation) {
        profiler.startTimer();
        profiler.setAnnotation(QueryProfiler.QUERY_ANNOTATION, query.getMixedQuery());
        Number result = serializer.queryAggregation(query, txHandle, aggregation);
        profiler.stopTimer();
        return result;
    }

    @Override
    public void observeWith(QueryProfiler parentProfiler, boolean hasSiblings) {
        profiler = parentProfiler.addNested(QueryProfiler.MIXED_INEX_COUNT_QUERY);
    }

    @Override
    public QueryProfiler getProfiler() {
        return profiler;
    }
}
