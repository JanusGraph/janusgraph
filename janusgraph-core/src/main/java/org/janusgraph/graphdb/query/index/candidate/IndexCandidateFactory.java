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

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

public class IndexCandidateFactory {

    /**
     * Creates an <code>IndexCandidate</code> from a <code>MultiCondition</code> which it covers.
     * @param subCover For the condition to be valid, it needs to match these rules:
     *                   <ul>
     *                   <li>It must be an equality condition</li>
     *                   <li>It must not cover multiple labels</li>
     *                   <li>The label must match the given <code>index</code></li>
     *                   </ul>
     * @return An instance of <code>IndexCandidate</code> if the parameter <code>conditions</code> is valid, <code>null</code> else.
     */
    @Nullable
    public static <E extends JanusGraphElement> AbstractIndexCandidate<? extends IndexType> build(final IndexType index,
                                                                                                     final Set<Condition> subCover,
                                                                                                     Object subCondition,
                                                                                                     OrderList orders) {
        if (index.isMixedIndex()) {
            return new MixedIndexCandidate((MixedIndexType) index, subCover, (Condition) subCondition, orders);
        } else {
            return new CompositeIndexCandidate((CompositeIndexType) index, subCover, (List<Object[]>) subCondition, orders);
        }
    }
}
