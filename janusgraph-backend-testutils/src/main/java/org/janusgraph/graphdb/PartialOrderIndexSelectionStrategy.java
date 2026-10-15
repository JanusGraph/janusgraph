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

import com.google.common.collect.ImmutableList;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.index.BruteForceIndexSelectionStrategy;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;

import java.util.Set;

/**
 * An index selection strategy which gives the first index it selects only the first key of the order by which it holds
 * that index to sort the results, as a custom strategy might: the index then sorts the ties of that key in its own
 * order rather than by the order's other keys.
 */
public class PartialOrderIndexSelectionStrategy extends BruteForceIndexSelectionStrategy {

    public PartialOrderIndexSelectionStrategy(Configuration config) {
        super(config);
    }

    @Override
    public SelectedIndexQuery selectIndices(final Set<IndexType> rawCandidates,
                                            final MultiCondition<JanusGraphElement> conditions,
                                            final Set<Condition> coveredClauses, OrderList orders,
                                            IndexSerializer serializer) {
        final SelectedIndexQuery selected = super.selectIndices(rawCandidates, conditions, coveredClauses, orders,
            serializer);
        final JointIndexQuery query = selected.getQuery();
        if (query.isEmpty() || !query.getQuery(0).getIndex().isMixedIndex()
                || query.getQuery(0).getMixedQuery().getOrder().size() < 2) {
            return selected;
        }
        final JointIndexQuery partial = new JointIndexQuery();
        for (int i = 0; i < query.size(); i++) {
            final JointIndexQuery.Subquery subquery = query.getQuery(i);
            if (subquery.getIndex().isCompositeIndex()) {
                partial.add((CompositeIndexType) subquery.getIndex(), subquery.getCompositeQuery());
            } else if (i == 0) {
                final IndexQuery mixed = subquery.getMixedQuery();
                partial.add((MixedIndexType) subquery.getIndex(), new IndexQuery(mixed.getStore(),
                    mixed.getCondition(), ImmutableList.of(mixed.getOrder().get(0))));
            } else {
                partial.add((MixedIndexType) subquery.getIndex(), subquery.getMixedQuery());
            }
        }
        return new SelectedIndexQuery(partial, selected.isSorted());
    }
}
