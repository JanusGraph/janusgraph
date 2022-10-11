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

package org.janusgraph.graphdb.query.index.candidate;

import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.index.IndexSelectionUtil;
import org.janusgraph.graphdb.types.MixedIndexType;

import java.util.Set;

public class MixedIndexCandidate extends AbstractIndexCandidate<MixedIndexType> {

    private final Condition subCondition;

    public MixedIndexCandidate(MixedIndexType index, Set<Condition> subCover, Condition subCondition, OrderList orders) {
        super(index, subCover, orders);
        this.subCondition = subCondition;
    }

    @Override
    public void addToJointQuery(JointIndexQuery query, IndexSerializer serializer) {
        query.add(index, serializer.getQuery(index, subCondition, orders));
    }

    @Override
    public boolean supportsOrders() {
        return IndexSelectionUtil.indexCoversOrder(index, orders);
    }
}
