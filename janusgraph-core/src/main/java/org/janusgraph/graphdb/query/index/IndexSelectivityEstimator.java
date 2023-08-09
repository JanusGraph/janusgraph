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
import org.janusgraph.core.PropertyKey;
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

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.StreamSupport;

public class IndexSelectivityEstimator {
    public static <E extends JanusGraphElement> double estimateSelectivity(final Condition<E> condition, IndexType index,
                                                                           Map<String, Double> userDefinedSelectivities) {
        if (condition instanceof Literal) {
            return estimateSelectivity((Literal<E>) condition, index, userDefinedSelectivities);
        } else if (condition instanceof Not) {
            return 1 - estimateSelectivity(((Not<E>) condition).getChild(), index, userDefinedSelectivities);
        } else if (condition instanceof And) {
            return independentIntersection(condition.getChildren(), subCondition ->
                estimateSelectivity(subCondition, index, userDefinedSelectivities));
        } else if (condition instanceof Or) {
            return independentUnion(condition.getChildren(), subCondition ->
                estimateSelectivity(subCondition, index, userDefinedSelectivities));
        } else {
            throw new IllegalArgumentException("Condition " + condition + " has unsupported type");
        }
    }

    private static <E extends JanusGraphElement> double estimateSelectivity(Literal<E> literal, IndexType index,
                                                                            Map<String, Double> userDefinedSelectivities) {
        if (literal instanceof PredicateCondition) {
            PredicateCondition<PropertyKey, E> predicateCondition = (PredicateCondition<PropertyKey, E>) literal;
            Optional<Double> userDefinedSelectivity = Optional.ofNullable(userDefinedSelectivities.get(predicateCondition.getKey().name()));
            return estimateSelectivity(predicateCondition.getPredicate(), index, userDefinedSelectivity);
        } else {
            throw new IllegalArgumentException("Literal " + literal + " has unsupported type");
        }
    }

    private static double estimateSelectivity(JanusGraphPredicate predicate, IndexType index,
                                              Optional<Double> userDefinedSelectivity) {
        if (predicate instanceof Cmp) {
            return estimateSelectivity((Cmp) predicate, index, userDefinedSelectivity);
        } else if (predicate instanceof Contain) {
            return estimateSelectivity((Contain) predicate, index, userDefinedSelectivity);
        } else if (predicate instanceof Text) {
            return estimateSelectivity((Text) predicate, index, userDefinedSelectivity);
        } else if (predicate instanceof Geo) {
            return estimateSelectivity((Geo) predicate, index, userDefinedSelectivity);
        } else if (predicate instanceof AndJanusPredicate) {
            double selectivity = 1.0;
            for (JanusGraphPredicate subPredicate : ((AndJanusPredicate) predicate)) {
                selectivity *= estimateSelectivity(subPredicate, index, userDefinedSelectivity);
            }
            return selectivity;
        } else if (predicate instanceof OrJanusPredicate) {
            return independentUnion((OrJanusPredicate) predicate,
                subPredicate -> estimateSelectivity(subPredicate, index, userDefinedSelectivity));
        } else {
            throw new IllegalArgumentException("Predicate " + predicate + " has unsupported type");
        }
    }

    private static double estimateSelectivity(Cmp cmp, IndexType index, Optional<Double> userDefinedSelectivity) {
        boolean uniqueIndex = index.isCompositeIndex() && ((CompositeIndexType) index).getCardinality() == Cardinality.SINGLE;
        double positiveSelectivity = userDefinedSelectivity.orElse(uniqueIndex ? 0.0 : 0.1);
        switch (cmp) {
            case EQUAL: return positiveSelectivity;
            case NOT_EQUAL: return 1.0 - positiveSelectivity;
            default: return 0.5;
        }
    }

    private static double estimateSelectivity(Contain contain, IndexType index,
                                              Optional<Double> userDefinedSelectivity) {
        boolean uniqueIndex = index.isCompositeIndex() && ((CompositeIndexType) index).getCardinality() == Cardinality.SINGLE;
        // waive smarter estimation by using count of values for now, just make it a bit worse than EQUAL/NOT_EQUAL
        double positiveSelectivity = userDefinedSelectivity.orElse(uniqueIndex ? 0.0 : 0.2);
        switch (contain) {
            case IN: return positiveSelectivity;
            case NOT_IN: return 1.0 - positiveSelectivity;
            default: throw new IllegalArgumentException("Contain " + contain + " has unsupported type");
        }
    }

    private static double estimateSelectivity(Text ignoredText, IndexType ignoredIndex,
                                              Optional<Double> userDefinedSelectivity) {
        // waive smarter estimation for now
        return userDefinedSelectivity.orElse(0.5);
    }

    private static double estimateSelectivity(Geo ignoredGeo, IndexType ignoredIndex,
                                              Optional<Double> userDefinedSelectivity) {
        // waive smarter estimation for now
        return userDefinedSelectivity.orElse(0.5);
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
