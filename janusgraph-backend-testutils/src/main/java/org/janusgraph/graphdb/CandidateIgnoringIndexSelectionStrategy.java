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
import org.janusgraph.graphdb.query.index.BruteForceIndexSelectionStrategy;
import org.janusgraph.graphdb.query.index.IndexSelectionUtil;
import org.janusgraph.graphdb.types.IndexType;

import java.util.Set;

/**
 * An index selection strategy which chooses among the indexes that match the conditions other than the candidates it
 * is given, where there are any, as a custom strategy which collects indexes of its own might: it takes an index which
 * the caller left out.
 */
public class CandidateIgnoringIndexSelectionStrategy extends BruteForceIndexSelectionStrategy {

    public CandidateIgnoringIndexSelectionStrategy(Configuration config) {
        super(config);
    }

    @Override
    public SelectedIndexQuery selectIndices(final Set<IndexType> rawCandidates,
                                            final MultiCondition<JanusGraphElement> conditions,
                                            final Set<Condition> coveredClauses, OrderList orders,
                                            IndexSerializer serializer) {
        final Set<IndexType> others = IndexSelectionUtil.getMatchingIndexes(conditions);
        others.removeAll(rawCandidates);
        return super.selectIndices(others.isEmpty() ? rawCandidates : others, conditions, coveredClauses, orders,
            serializer);
    }
}
