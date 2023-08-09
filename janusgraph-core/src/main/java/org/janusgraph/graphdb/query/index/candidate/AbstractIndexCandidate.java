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
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.index.CostBasedIndexSelector;
import org.janusgraph.graphdb.query.index.IndexSelectionUtil;
import org.janusgraph.graphdb.query.index.IndexSelectivityEstimator;
import org.janusgraph.graphdb.types.IndexType;

import java.util.Map;
import java.util.Set;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public abstract class AbstractIndexCandidate<I extends IndexType, E extends JanusGraphElement> {
    protected final I index;
    private final Set<Condition<E>> subCover;
    protected OrderList orders;

    public AbstractIndexCandidate(final I index, final Set<Condition<E>> subCover, OrderList orders) {
        this.index = index;
        this.subCover = subCover;
        this.orders = orders;
    }

    public I getIndex() {
        return index;
    }

    public Set<Condition<E>> getSubCover() {
        return subCover;
    }

    public double estimateSelectivity(Map<String, Double> userDefinedSelectivities) {
        return IndexSelectivityEstimator.independentIntersection(subCover,
            c -> IndexSelectivityEstimator.estimateSelectivity(c, index, userDefinedSelectivities));
    }

    public double estimateCost(boolean ignoreOrder, Map<String, Double> userDefinedSelectivities) {
        double cost = estimateSelectivity(userDefinedSelectivities) * IndexSelectionUtil.costFactor(index);
        return ignoreOrder ? cost : cost * CostBasedIndexSelector.MANUAL_ORDER_PENALTY;
    }

    public abstract void addToJointQuery(final JointIndexQuery query, final IndexSerializer serializer);

    public abstract boolean supportsOrders();
}
