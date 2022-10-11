// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.query.index;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.index.candidate.AbstractIndexCandidate;
import org.janusgraph.graphdb.query.index.candidate.IndexCandidateFactory;
import org.janusgraph.graphdb.query.index.candidate.IndexCandidateGroup;
import org.janusgraph.graphdb.types.IndexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CostBasedIndexSelector {
    public static <E extends JanusGraphElement> SelectedIndexQuery<E> selectIndices(ElementCategory resultType,
                                                                                    MultiCondition<E> conditions,
                                                                                    OrderList orders,
                                                                                    IndexSerializer serializer) {
        Set<IndexType> rawCandidates = IndexSelectionUtil.getMatchingIndexes(conditions, resultType, serializer);
        final Set<AbstractIndexCandidate<?,E>> remainingCandidates = new HashSet<>(rawCandidates.size());
        final Set<Condition<E>> allCoverableClauses = new HashSet<>();
        final JointIndexQuery jointQuery = new JointIndexQuery();

        // validate, enrich index candidates
        for (final IndexType index : rawCandidates) {
            AbstractIndexCandidate<?,E> ic = IndexCandidateFactory.build(index, conditions, serializer, orders);
            if (ic == null) continue;
            remainingCandidates.add(ic);
            allCoverableClauses.addAll(ic.getSubCover());
        }

        IndexCandidateGroup<E> bestGroup = new IndexCandidateGroup<>(Collections.emptyList(), orders);

        // select indexes in a greedy fashion
        while (true) {
            double lowestNewCost = Double.POSITIVE_INFINITY;
            AbstractIndexCandidate<?,E> bestNewCandidate = null;

            for (AbstractIndexCandidate<?,E> newCandidate : remainingCandidates) {
                List<AbstractIndexCandidate<?,E>> extendedCandidates = new ArrayList<>(bestGroup.getIndexCandidates());
                extendedCandidates.add(newCandidate);
                // build new group of index candidates and compare it to the best group found so far
                double newCost = 0; // TODO: estimate cost of new group
                if (newCost < lowestNewCost) {
                    lowestNewCost = newCost;
                    bestNewCandidate = newCandidate;
                }
            }

            if (bestNewCandidate != null && lowestNewCost < 0) { // TODO: compare to new cost
                bestGroup.addCandidate(bestNewCandidate);
                remainingCandidates.remove(bestNewCandidate);
            } else {
                break; // unable to improve cost by querying any additional indexes
            }
        }

        // build query
        bestGroup.getIndexCandidates().forEach(c -> c.addToJointQuery(jointQuery, serializer));
        return new SelectedIndexQuery<>(jointQuery, bestGroup.getCoveredClauses(), bestGroup.supportsOrders());
    }
}
