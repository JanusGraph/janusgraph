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
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
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
import java.util.Optional;
import java.util.Set;

public class CostBasedIndexSelector {
    public static final double MANUAL_FILTER_PENALTY = 4;
    public static final double MANUAL_ORDER_PENALTY = 4;

    public static <E extends JanusGraphElement> SelectedIndexQuery<E> selectIndices(ElementCategory resultType,
                                                                                    MultiCondition<E> conditions,
                                                                                    OrderList orders,
                                                                                    IndexSerializer serializer,
                                                                                    Configuration hints) {
        Set<IndexType> rawCandidates = IndexSelectionUtil.getMatchingIndexes(conditions, resultType, serializer);
        final Set<AbstractIndexCandidate<?,E>> remainingCandidates = new HashSet<>(rawCandidates.size());
        final Set<Condition<E>> allCoverableClauses = new HashSet<>();
        final JointIndexQuery jointQuery = new JointIndexQuery();

        removeSuppressedIndexes(rawCandidates, hints.get(GraphDatabaseConfiguration.SUPPRESSED_INDEXES));

        // validate, enrich index candidates
        for (final IndexType index : rawCandidates) {
            AbstractIndexCandidate<?,E> ic = IndexCandidateFactory.build(index, conditions, serializer, orders);
            if (ic == null) continue;
            remainingCandidates.add(ic);
            allCoverableClauses.addAll(ic.getSubCover());
        }

        IndexCandidateGroup<E> bestGroup = new IndexCandidateGroup<>(Collections.emptyList(), orders);

        selectPreferredIndexes(bestGroup, remainingCandidates, hints.get(GraphDatabaseConfiguration.PREFERRED_INDEXES));

        // select indexes in a greedy fashion
        while (true) {
            double lowestNewCost = Double.POSITIVE_INFINITY;
            AbstractIndexCandidate<?,E> bestNewCandidate = null;

            for (AbstractIndexCandidate<?,E> newCandidate : remainingCandidates) {
                List<AbstractIndexCandidate<?,E>> extendedCandidates = new ArrayList<>(bestGroup.getIndexCandidates());
                extendedCandidates.add(newCandidate);
                // build new group of index candidates and compare it to the best group found so far
                double newCost = new IndexCandidateGroup<>(extendedCandidates, orders).estimateTotalCost(allCoverableClauses);
                if (newCost < lowestNewCost) {
                    lowestNewCost = newCost;
                    bestNewCandidate = newCandidate;
                }
            }

            if (bestNewCandidate != null && lowestNewCost < bestGroup.estimateTotalCost(allCoverableClauses)) {
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

    private static void removeSuppressedIndexes(Set<IndexType> remainingCandidates, String[] suppressedIndexNames) {
        if (suppressedIndexNames == null) return;
        for (String indexName : suppressedIndexNames) {
            remainingCandidates.stream()
                .filter(i -> i.getName().equals(indexName.trim()))
                .findFirst()
                .ifPresent(remainingCandidates::remove);
        }
    }

    private static <E extends JanusGraphElement> void selectPreferredIndexes(IndexCandidateGroup<E> bestGroup,
                                                                             Set<AbstractIndexCandidate<?,E>> remainingCandidates,
                                                                             String[] preferredIndexNames) {
        if (preferredIndexNames == null) return;
        for (String indexName : preferredIndexNames) {
            Optional<AbstractIndexCandidate<?, E>> index = remainingCandidates.stream()
                .filter(i -> i.getIndex().getName().equals(indexName.trim()))
                .findFirst();
            index.ifPresent(bestGroup::addCandidate);
            index.ifPresent(remainingCandidates::remove);
        }
    }
}
