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

import java.util.HashSet;
import java.util.Set;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class IndexCandidateGroup implements Comparable<IndexCandidateGroup> {

    private Set<IndexCandidate> indexCandidates = new HashSet<>();

    private Set<Condition> coveredClauses = new HashSet<>();

    private double totalScore;

    public Set<IndexCandidate> getIndexCandidates() {
        return indexCandidates;
    }

    public void setIndexCandidates(Set<IndexCandidate> indexCandidates) {
        this.indexCandidates = indexCandidates;
    }

    public Set<Condition> getCoveredClauses() {
        return coveredClauses;
    }

    public void setCoveredClauses(Set<Condition> coveredClauses) {
        this.coveredClauses = coveredClauses;
    }

    public double getTotalScore() {
        return totalScore;
    }

    public void setTotalScore(double totalScore) {
        this.totalScore = totalScore;
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
        return Double.compare(totalScore, that.totalScore);
    }
}
