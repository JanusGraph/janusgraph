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

package org.janusgraph.graphdb.query.index;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.util.datastructures.PowerSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class BruteForceIndexSelectionStrategy
    extends AbstractIndexSelectionStrategy {

    public static final String NAME = "brute-force";

    public BruteForceIndexSelectionStrategy(Configuration config) {
       super(config);
    }

    /**
     * Determine the best jointIndexQuery by enumerating all possibilities with exponential time
     * complexity. Similar to Weighted Set Cover problem, to find the best choice is NP-Complete, so
     * we should be careful that the problem size MUST be small, otherwise it is more recommended to
     * use an approximation algorithm.
     */
    @Override
    public SelectedIndexQuery selectIndices(final Set<IndexType> rawCandidates,
                                            final MultiCondition<JanusGraphElement> conditions,
                                            final Set<Condition> coveredClauses, OrderList orders,
                                            IndexSerializer serializer) {
        final JointIndexQuery jointQuery = new JointIndexQuery();
        final Set<IndexCandidate> indexCandidates = new HashSet<>();
        boolean isSorted = orders.isEmpty();

        // validate, enrich index candidates and calculate scores
        for (final IndexType index : rawCandidates) {
            IndexCandidate ic = createIndexCandidate(index, conditions, serializer);
            if (ic == null) {
                continue;
            }
            ic.setScore(calculateIndexCandidateScore(ic));
            indexCandidates.add(ic);
        }

        IndexCandidateGroup bestGroup = null;
        for (Set<IndexCandidate> subset : new PowerSet<>(indexCandidates)) {
            if (subset.isEmpty())
                continue;
            final IndexCandidateGroup group = new IndexCandidateGroup(subset);
            if (group.compareTo(bestGroup) > 0) {
                bestGroup = group;
            }
        }

        if (bestGroup != null) {
            coveredClauses.addAll(bestGroup.getCoveredClauses());
            List<IndexCandidate> bestIndexes = new ArrayList<>(bestGroup.getIndexCandidates());
            // sort indexes by score descending order
            bestIndexes.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));
            // isSorted depends on the first index subquery
            isSorted =
                orders.isEmpty() || bestIndexes.get(0).getIndex().isMixedIndex() &&
                                        IndexSelectionUtil.indexCoversOrder(
                                            (MixedIndexType) bestIndexes.get(0).getIndex(), orders);
            for (IndexCandidate c : bestIndexes) {
                addToJointQuery(c, jointQuery, serializer, orders);
            }
        }

        return new SelectedIndexQuery(jointQuery, isSorted);
    }

    private double calculateIndexCandidateScore(final IndexCandidate ic) {
        double score = 0.0;
        for (final Condition c : ic.getSubCover()) {
            score += getConditionBasicScore(c) + getIndexTypeScore(ic.getIndex());
        }
        return score;
    }
}
