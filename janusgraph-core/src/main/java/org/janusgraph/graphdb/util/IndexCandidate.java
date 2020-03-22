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

package org.janusgraph.graphdb.util;

import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.types.IndexType;

import java.util.Set;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class IndexCandidate {
    private final IndexType index;

    private final Set<Condition> subcover;

    private final Object subCondition;

    private double score;

    public IndexCandidate(IndexType index, Set<Condition> subcover, Object subCondition, double score) {
        this.index = index;
        this.subcover = subcover;
        this.subCondition = subCondition;
        this.score = score;
    }

    public IndexType getIndex() {
        return index;
    }

    public Set<Condition> getSubcover() {
        return subcover;
    }

    public Object getSubCondition() {
        return subCondition;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
