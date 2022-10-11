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

import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.types.IndexType;

import java.util.Set;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public abstract class AbstractIndexCandidate<I extends IndexType> {
    protected final I index;
    private final Set<Condition> subCover;
    protected OrderList orders;

    // initialize with the worst possible score
    private double score = Double.NEGATIVE_INFINITY;

    public AbstractIndexCandidate(final I index, final Set<Condition> subCover, OrderList orders) {
        this.index = index;
        this.subCover = subCover;
        this.orders = orders;
    }

    public I getIndex() {
        return index;
    }
    public Set<Condition> getSubCover() {
        return subCover;
    }
    public void setScore(double newScore) { this.score = newScore; }
    public double getScore() {
        return score;
    }

    public abstract void addToJointQuery(final JointIndexQuery query, final IndexSerializer serializer);

    public abstract boolean supportsOrders();
}
