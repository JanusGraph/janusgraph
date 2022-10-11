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

import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class IndexCandidateGroup implements Comparable<IndexCandidateGroup> {

    private Set<AbstractIndexCandidate> indexCandidates;
    private Set<Condition> coveredClauses;
    private final OrderList orders;

    // initialize with the worst possible score
    private double score = Double.NEGATIVE_INFINITY;

    public IndexCandidateGroup(Set<AbstractIndexCandidate> indexCandidates, OrderList orders) {
        this.indexCandidates = new HashSet<>();
        this.coveredClauses = new HashSet<>();
        this.orders = orders;

        indexCandidates.forEach(this::addCandidate);
    }

    public void addCandidate(AbstractIndexCandidate newCandidate) {
        Set<Condition> newClauses = newCandidate.getSubCover();
        newClauses.removeAll(coveredClauses);

        indexCandidates.add(newCandidate);
        coveredClauses.addAll(newClauses);
    }

    public Set<AbstractIndexCandidate> getIndexCandidates() {
        return indexCandidates;
    }

    public Set<Condition> getCoveredClauses() {
        return coveredClauses;
    }

    public double getTotalScore() {
        if (score == Double.NEGATIVE_INFINITY) {
            score = indexCandidates.stream().mapToDouble(AbstractIndexCandidate::getScore).sum();
        }

        return score;
    }

    /**
     * Covering more clauses, using fewer indices, and getting higher score is better
     *
     * @param that
     * @return
     */
    @Override
    public int compareTo(IndexCandidateGroup that) {
        if (that == null) return 1;
        if (coveredClauses.size() > that.getCoveredClauses().size()) return 1;
        if (coveredClauses.size() < that.getCoveredClauses().size()) return -1;
        if (indexCandidates.size() < that.getIndexCandidates().size()) return 1;
        if (indexCandidates.size() > that.getIndexCandidates().size()) return -1;
        return Double.compare(getTotalScore(), that.getTotalScore());
    }
}
