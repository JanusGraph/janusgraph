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

package org.janusgraph.graphdb.query.index.candidate;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.index.CostBasedIndexSelector;
import org.janusgraph.graphdb.query.index.IndexSelectivityEstimator;
import org.janusgraph.graphdb.types.IndexType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class IndexCandidateGroup<E extends JanusGraphElement> {

    private final List<AbstractIndexCandidate<?,E>> indexCandidates;
    private final Map<Condition<E>, IndexType> indexByClause;
    private final Set<Condition<E>> coveredClauses;
    private final OrderList orders;

    public IndexCandidateGroup(Collection<AbstractIndexCandidate<?,E>> indexCandidates, OrderList orders) {
        this.indexCandidates = new ArrayList<>();
        this.indexByClause = new HashMap<>();
        this.coveredClauses = new HashSet<>();
        this.orders = orders;

        indexCandidates.forEach(this::addCandidate);
    }

    public void addCandidate(AbstractIndexCandidate<?,E> newCandidate) {
        Set<Condition<E>> newClauses = newCandidate.getSubCover();
        newClauses.removeAll(coveredClauses);

        indexCandidates.add(newCandidate);
        newClauses.forEach(cl -> indexByClause.put(cl, newCandidate.getIndex()));
        coveredClauses.addAll(newClauses);
    }

    public List<AbstractIndexCandidate<?,E>> getIndexCandidates() {
        return Collections.unmodifiableList(indexCandidates);
    }

    public Set<Condition<E>> getCoveredClauses() {
        return coveredClauses;
    }

    public double estimateTotalCost(Set<Condition<E>> allClauses, Map<String, Double> userDefinedSelectivities) {
        double indexQueryCost = 0;

        for (AbstractIndexCandidate<?,E> c : indexCandidates) {
            indexQueryCost += c.estimateCost(true, userDefinedSelectivities);
        }

        double estimatedIndexSelectivity = IndexSelectivityEstimator.independentIntersection(coveredClauses,
            c -> IndexSelectivityEstimator.estimateSelectivity(c, indexByClause.get(c), userDefinedSelectivities));

        Set<Condition<E>> uncoveredClauses = new HashSet<>(allClauses);
        uncoveredClauses.removeAll(coveredClauses);
        double manualFilterPenalty = estimatedIndexSelectivity * uncoveredClauses.size() * CostBasedIndexSelector.MANUAL_FILTER_PENALTY;

        double estimatedTotalSelectivity = IndexSelectivityEstimator.independentIntersection(coveredClauses,
            c -> IndexSelectivityEstimator.estimateSelectivity(c, indexByClause.get(c), userDefinedSelectivities));

        boolean supportsOrders = orders.isEmpty() || (!indexCandidates.isEmpty() && indexCandidates.get(0).supportsOrders());
        double orderPenalty = supportsOrders ? 0 : estimatedTotalSelectivity * CostBasedIndexSelector.MANUAL_ORDER_PENALTY;

        return indexQueryCost + manualFilterPenalty + orderPenalty;
    }

    public boolean supportsOrders() {
        if (indexCandidates.isEmpty()) return orders.isEmpty();
        return indexCandidates.get(0).supportsOrders();
    }
}
