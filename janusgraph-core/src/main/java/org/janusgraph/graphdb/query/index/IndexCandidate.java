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

import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.types.IndexType;

import java.util.Set;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class IndexCandidate {
    private final IndexType index;
    private final Set<Condition> subCover;
    private final Object subCondition;

    // initialize with the worst possible score
    private double score = Double.NEGATIVE_INFINITY;

    public IndexCandidate(final IndexType index,
                          final Set<Condition> subCover,
                          final Object subCondition) {
        this.index = index;
        this.subCover = subCover;
        this.subCondition = subCondition;
    }

    public IndexType getIndex() {
        return index;
    }
    public Set<Condition> getSubCover() {
        return subCover;
    }
    public Object getSubCondition() {
        return subCondition;
    }
    public void setScore(double newScore) { this.score = newScore; }
    public double getScore() {
        return score;
    }
}
