// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.index.BruteForceIndexSelectionStrategy;
import org.janusgraph.graphdb.query.index.IndexSelectionUtil;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;

import java.util.Set;

/**
 * An index selection strategy which decides whether the first index it selects sorts the results by the deprecated
 * {@link IndexSelectionUtil#indexCoversOrder(MixedIndexType, OrderList)}, as one written before it took the
 * {@link IndexSerializer} would: it holds an index to sort by any key which the index holds, whether or not the index
 * backend can sort by it, as by a key of several values or a string key of TEXT mapping.
 */
public class DeprecatedOrderCoverageIndexSelectionStrategy extends BruteForceIndexSelectionStrategy {

    public DeprecatedOrderCoverageIndexSelectionStrategy(Configuration config) {
        super(config);
    }

    @Override
    @SuppressWarnings("deprecation")
    public SelectedIndexQuery selectIndices(final Set<IndexType> rawCandidates,
                                            final MultiCondition<JanusGraphElement> conditions,
                                            final Set<Condition> coveredClauses, OrderList orders,
                                            IndexSerializer serializer) {
        final SelectedIndexQuery selected = super.selectIndices(rawCandidates, conditions, coveredClauses, orders,
            serializer);
        final JointIndexQuery query = selected.getQuery();
        final boolean isSorted = orders.isEmpty() || !query.isEmpty() && query.getQuery(0).getIndex().isMixedIndex()
            && IndexSelectionUtil.indexCoversOrder((MixedIndexType) query.getQuery(0).getIndex(), orders);
        return new SelectedIndexQuery(query, isSorted);
    }
}
