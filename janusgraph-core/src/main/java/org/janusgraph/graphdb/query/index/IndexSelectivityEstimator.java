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

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.predicate.AndJanusPredicate;
import org.janusgraph.graphdb.predicate.OrJanusPredicate;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.Literal;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;

import java.util.function.Function;
import java.util.stream.StreamSupport;

public class IndexSelectivityEstimator {
    public static <E extends JanusGraphElement> double estimateSelectivity(final Condition<E> condition, IndexType index) {
        if (condition instanceof Literal) {
            return estimateSelectivity((Literal<E>) condition, index);
        } else if (condition instanceof Not) {
            return 1 - estimateSelectivity(((Not<E>) condition).getChild(), index);
        } else if (condition instanceof And) {
            return independentIntersection(condition.getChildren(), subCondition -> estimateSelectivity(subCondition, index));
        } else if (condition instanceof Or) {
            return independentUnion(condition.getChildren(), subCondition -> estimateSelectivity(subCondition, index));
        } else {
            throw new IllegalArgumentException("Condition " + condition + " has unsupported type");
        }
    }

    private static <E extends JanusGraphElement> double estimateSelectivity(Literal<E> literal, IndexType index) {
        if (literal instanceof PredicateCondition) {
            return estimateSelectivity(((PredicateCondition<?, E>) literal).getPredicate(), index);
        } else {
            throw new IllegalArgumentException("Literal " + literal + " has unsupported type");
        }
    }

    private static double estimateSelectivity(JanusGraphPredicate predicate, IndexType index) {
        if (predicate instanceof Cmp) {
            return estimateSelectivity((Cmp) predicate, index);
        } else if (predicate instanceof Contain) {
            return estimateSelectivity((Contain) predicate, index);
        } else if (predicate instanceof Text) {
            return estimateSelectivity((Text) predicate, index);
        } else if (predicate instanceof Geo) {
            return estimateSelectivity((Geo) predicate, index);
        } else if (predicate instanceof AndJanusPredicate) {
            double selectivity = 1.0;
            for (JanusGraphPredicate subPredicate : ((AndJanusPredicate) predicate)) {
                selectivity *= estimateSelectivity(subPredicate, index);
            }
            return selectivity;
        } else if (predicate instanceof OrJanusPredicate) {
            return independentUnion((OrJanusPredicate) predicate, subPredicate -> estimateSelectivity(subPredicate, index));
        } else {
            throw new IllegalArgumentException("Predicate " + predicate + " has unsupported type");
        }
    }

    private static double estimateSelectivity(Cmp cmp, IndexType index) {
        boolean uniqueIndex = index.isCompositeIndex() && ((CompositeIndexType) index).getCardinality() == Cardinality.SINGLE;
        switch (cmp) {
            case EQUAL: return uniqueIndex ? 0.0 : 0.1;
            case NOT_EQUAL: return 1.0;
            default: return 0.5;
        }
    }

    private static double estimateSelectivity(Contain contain, IndexType index) {
        boolean uniqueIndex = index.isCompositeIndex() && ((CompositeIndexType) index).getCardinality() == Cardinality.SINGLE;
        switch (contain) {
            case IN: return uniqueIndex ? 0.0 : 0.5;
            case NOT_IN: return uniqueIndex ? 1.0 : 0.5;
            default: throw new IllegalArgumentException("Contain " + contain + " has unsupported type");
        }
    }

    private static double estimateSelectivity(Text ignoredText, IndexType ignoredIndex) {
        // waive smarter estimation for now
        return 0.5;
    }

    private static double estimateSelectivity(Geo ignoredGeo, IndexType ignoredIndex) {
        // waive smarter estimation for now
        return 0.5;
    }

    public static <E> double independentIntersection(Iterable<E> items, Function<E,Double> itemEstimator) {
        return StreamSupport.stream(items.spliterator(), false)
            .map(itemEstimator)
            .reduce(1.0, (a, b) -> a * b);
    }

    public static <E> double independentUnion(Iterable<E> items, Function<E,Double> itemEstimator) {
        return 1.0 - StreamSupport.stream(items.spliterator(), false)
            .map(itemEstimator)
            .reduce(1.0, (a, b) -> a * (1.0 - b));
    }
}
