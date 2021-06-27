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
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.types.IndexType;

import java.util.Set;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public interface IndexSelectionStrategy {
    SelectedIndexQuery selectIndices(final Set<IndexType> indexCandidates,
                                     final MultiCondition<JanusGraphElement> conditions,
                                     final Set<Condition> coveredClauses, OrderList orders,
                                     IndexSerializer serializer);

    SelectedIndexQuery selectIndices(final ElementCategory resultType,
                                     final MultiCondition<JanusGraphElement> conditions,
                                     final Set<Condition> coveredClauses, OrderList orders,
                                     IndexSerializer serializer);
    class SelectedIndexQuery {
        private JointIndexQuery query;
        private boolean isSorted;

        public SelectedIndexQuery(JointIndexQuery query, boolean isSorted) {
            this.query = query;
            this.isSorted = isSorted;
        }

        public JointIndexQuery getQuery() { return query; }

        public boolean isSorted() { return isSorted; }
    }
}
