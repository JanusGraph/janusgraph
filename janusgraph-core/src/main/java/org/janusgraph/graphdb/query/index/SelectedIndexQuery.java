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
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;

import java.util.Collections;
import java.util.Set;

public class SelectedIndexQuery<E extends JanusGraphElement> {
    private final JointIndexQuery query;
    private final Set<Condition<E>> coveredClauses;
    private final boolean isSorted;

    public SelectedIndexQuery(JointIndexQuery query, Set<Condition<E>> coveredClauses, boolean isSorted) {
        this.query = query;
        this.coveredClauses = coveredClauses;
        this.isSorted = isSorted;
    }

    public JointIndexQuery getQuery() {
        return query;
    }

    public Set<Condition<E>> getCoveredClauses() {
        return Collections.unmodifiableSet(coveredClauses);
    }

    public boolean isSorted() {
        return isSorted;
    }
}
