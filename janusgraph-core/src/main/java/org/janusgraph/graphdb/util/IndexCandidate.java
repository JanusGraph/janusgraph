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

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class IndexCandidate {
    private static final double EQUAL_CONDITION_SCORE = 4;
    private static final double OTHER_CONDITION_SCORE = 1;
    private static final double CARDINALITY_SINGE_SCORE = 1000;
    private static final double CARDINALITY_OTHER_SCORE = 1000;

    private static final double ORDER_MATCH = 1;
    private static final double ALREADY_MATCHED_ADJUSTOR = -1.1;

    private final IndexType index;
    private final Set<Condition> subCover;
    private final Object subCondition;

    // initialize with the worst possible score
    private double score = Double.NEGATIVE_INFINITY;

    public IndexCandidate(final IndexType index,
                          final MultiCondition<JanusGraphElement> conditions,
                          final IndexSerializer serializer) {
        this.index = index;

        final Set<Condition> subCover = new HashSet<>(1);
        Object subCondition = null;

        // Check that this index actually applies in case of a schema constraint
        if (index.hasSchemaTypeConstraint()) {
            final JanusGraphSchemaType type = index.getSchemaTypeConstraint();
            final Map.Entry<Condition, Collection<Object>> equalCon =
                GraphCentricQueryBuilder.getEqualityConditionValues(conditions, ImplicitKey.LABEL);

            if (equalCon == null) {
                throw new IllegalArgumentException("Only equality conditions are supported");
            }

            final Collection<Object> labels = equalCon.getValue();
            assert labels.size() >= 1;

            if (labels.size() > 1) {
                throw new IllegalArgumentException("The query optimizer currently does not support multiple label constraints");
            }
            if (!type.name().equals(labels.iterator().next())) {
                throw new IllegalArgumentException("Given IndexType does not match given condition label");
            }
            subCover.add(equalCon.getKey());
        }

        if (index.isCompositeIndex()) {
            subCondition = GraphCentricQueryBuilder.indexCover((CompositeIndexType) index, conditions, subCover);
        } else {
            subCondition = GraphCentricQueryBuilder.indexCover((MixedIndexType) index, conditions, serializer, subCover);
        }
        if (subCondition == null || subCover.isEmpty()) {
            throw new IllegalArgumentException("Unable to initialize IndexCandidate from given parameters");
        }
        
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

    public void calculateScoreBruteForce() {
        score = 0.0;
        for (final Condition c : subCover) {
            score += getConditionBasicScore(c) + getIndexTypeScore(index);
        }
    }

    public void calculateScoreApproximation(final Set<Condition> coveredClauses, boolean supportsSort) {
        score = 0.0;

        for (final Condition c : subCover) {
            double subScore = getConditionBasicScore(c);
            if (coveredClauses.contains(c)) {
                subScore = subScore * ALREADY_MATCHED_ADJUSTOR;
            }
            score += subScore + getIndexTypeScore(index);
        }

        if (supportsSort) {
            score += ORDER_MATCH;
        }
    }

    public double getScore() {
        return score;
    }

    /**
     * Determines if this IndexCandidate only covers clauses, which are already covered by a given set.
     * @param coveringClauses
     * @return true if the IndexCandidate is fully covered by the given set. false if not.
     */
    public boolean isCoveredBy(final Set<Condition> coveringClauses) {
        for (final Condition c : subCover) {
            if (!coveringClauses.contains(c)) {
                return false;
            }
        }
        return true;
    }

    private double getConditionBasicScore(final Condition c) {
        if (c instanceof PredicateCondition && ((PredicateCondition) c).getPredicate() == Cmp.EQUAL) {
            return EQUAL_CONDITION_SCORE;
        } else {
            return OTHER_CONDITION_SCORE;
        }
    }

    private double getIndexTypeScore(final IndexType index) {
        double score = 0.0;
        if (index.isCompositeIndex()) {
            if (((CompositeIndexType)index).getCardinality() == Cardinality.SINGLE) {
                score = CARDINALITY_SINGE_SCORE;
            } else {
                score = CARDINALITY_OTHER_SCORE;
            }
        }
        return score;
    }
}
