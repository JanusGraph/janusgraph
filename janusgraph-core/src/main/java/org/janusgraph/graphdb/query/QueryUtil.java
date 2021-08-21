// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.query;

import com.google.common.base.Preconditions;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.predicate.AndJanusPredicate;
import org.janusgraph.graphdb.predicate.OrJanusPredicate;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods used in query optimization and processing.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class QueryUtil {

    public static int adjustLimitForTxModifications(StandardJanusGraphTx tx, int uncoveredAndConditions, int limit) {
        assert limit > 0;
        assert uncoveredAndConditions >= 0;

        if (uncoveredAndConditions > 0) {
            final int maxMultiplier = Integer.MAX_VALUE / limit;
            final int estimatedMultiplier = (int) Math.pow(2, uncoveredAndConditions);
            limit = estimatedMultiplier < maxMultiplier ? limit * estimatedMultiplier : Integer.MAX_VALUE;
        }

        if (tx.hasModifications())
            limit += Math.min(Integer.MAX_VALUE - limit, 5);

        return limit;
    }

    public static int convertLimit(long limit) {
        assert limit>=0 || limit==-1;
        if (limit>=Integer.MAX_VALUE || limit==-1) return Integer.MAX_VALUE;
        else return (int)limit;
    }

    public static int mergeLowLimits(int limit1, int limit2) {
        assert limit1>=0 && limit2>=0;
        return Math.max(limit1,limit2);
    }

    public static int mergeHighLimits(int limit1, int limit2) {
        assert limit1>=0 && limit2>=0;
        return Math.min(limit1,limit2);
    }

    public static InternalRelationType getType(StandardJanusGraphTx tx, String typeName) {
        final RelationType t = tx.getRelationType(typeName);
        if (t == null && !tx.getConfiguration().getAutoSchemaMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return (InternalRelationType) t;
    }

    public static Iterable<JanusGraphVertex> getVertices(StandardJanusGraphTx tx,
                                                    PropertyKey key, Object equalityCondition) {
        return tx.query().has(key,Cmp.EQUAL,equalityCondition).vertices();
    }

    public static Iterable<JanusGraphVertex> getVertices(StandardJanusGraphTx tx,
                                                    String key, Object equalityCondition) {
        return tx.query().has(key,Cmp.EQUAL,equalityCondition).vertices();
    }

    public static Iterable<JanusGraphEdge> getEdges(StandardJanusGraphTx tx,
                                                    PropertyKey key, Object equalityCondition) {
        return tx.query().has(key,Cmp.EQUAL,equalityCondition).edges();
    }

    public static Iterable<JanusGraphEdge> getEdges(StandardJanusGraphTx tx,
                                               String key, Object equalityCondition) {
        return tx.query().has(key,Cmp.EQUAL,equalityCondition).edges();
    }

    /**
     * Query-normal-form (QNF) for JanusGraph is a variant of CNF (conjunctive normal form) with negation inlined where possible
     *
     * @param condition
     * @return
     */
    public static boolean isQueryNormalForm(Condition<?> condition) {
        if (isQNFLiteralOrNot(condition)) {
            return true;
        }
        if (!(condition instanceof And)) {
            return false;
        }
        for (final Condition<?> child : ((And<?>) condition).getChildren()) {
            if (!isQNFLiteralOrNot(child)) {
                if (child instanceof Or) {
                    for (final Condition<?> child2 : ((Or<?>) child).getChildren()) {
                        if (!isQNFLiteralOrNot(child2)) return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isQNFLiteralOrNot(Condition<?> condition) {
        if (!(condition instanceof Not)) {
            return isQNFLiteral(condition);
        }
        final Condition child = ((Not) condition).getChild();
        return isQNFLiteral(child) && (!(child instanceof PredicateCondition) || !((PredicateCondition) child).getPredicate().hasNegation());
    }

    private static boolean isQNFLiteral(Condition<?> condition) {
        return condition.getType() == Condition.Type.LITERAL && (!(condition instanceof PredicateCondition) || ((PredicateCondition) condition).getPredicate().isQNF());
    }

    public static <E extends JanusGraphElement> Condition<E> simplifyAnd(And<E> condition) {
        if (condition.numChildren() == 1) {
            final Condition<E> child = condition.get(0);
            if (child.getType() == Condition.Type.LITERAL) return child;
        }
        return condition;
    }

    public static boolean isEmpty(Condition<?> condition) {
        return condition.getType() != Condition.Type.LITERAL && condition.numChildren() == 0;
    }

    /**
     * Prepares the constraints from the query builder into a QNF compliant condition.
     * If the condition is invalid or trivially false, it returns null.
     *
     * @param tx
     * @param constraints
     * @param <E>
     * @return
     * @see #isQueryNormalForm(org.janusgraph.graphdb.query.condition.Condition)
     */
    public static <E extends JanusGraphElement> And<E> constraints2QNF(StandardJanusGraphTx tx, List<PredicateCondition<String, E>> constraints) {
        final And<E> conditions = new And<>(constraints.size() + 4);
        for (final PredicateCondition<String, E> atom : constraints) {
            final RelationType type = getType(tx, atom.getKey());

            if (type == null) {
                if (atom.getPredicate() == Cmp.EQUAL && atom.getValue() == null)
                    continue; //Ignore condition, its trivially satisfied

                return null;
            }

            final Object value = atom.getValue();
            final JanusGraphPredicate predicate = atom.getPredicate();


            if (type.isPropertyKey()) {
                final PropertyKey key = (PropertyKey) type;
                assert predicate.isValidCondition(value);
                Preconditions.checkArgument(key.dataType()==Object.class || predicate.isValidValueType(key.dataType()), "Data type of key is not compatible with condition");
            } else { //its a label
                Preconditions.checkArgument(((EdgeLabel) type).isUnidirected());
                Preconditions.checkArgument(predicate.isValidValueType(JanusGraphVertex.class), "Data type of key is not compatible with condition");
            }

            if (predicate instanceof Contain) {
                //Rewrite contains conditions
                final Collection values = (Collection) value;
                if (predicate == Contain.NOT_IN) {
                    if (values.isEmpty()) continue; //Simply ignore since trivially satisfied
                    for (final Object inValue : values)
                        addConstraint(type, Cmp.NOT_EQUAL, inValue, conditions, tx);
                } else {
                    Preconditions.checkArgument(predicate == Contain.IN);
                    if (values.isEmpty()) {
                        return null; //Cannot be satisfied
                    } if (values.size() == 1) {
                        addConstraint(type, Cmp.EQUAL, values.iterator().next(), conditions, tx);
                    } else {
                        final Or<E> nested = new Or<>(values.size());
                        for (final Object invalue : values)
                            addConstraint(type, Cmp.EQUAL, invalue, nested, tx);
                        conditions.add(nested);
                    }
                }
            } else if (predicate instanceof AndJanusPredicate) {
                if (addConstraint(type, (AndJanusPredicate) (predicate), (List<Object>) (value), conditions, tx) == null) {
                    return null;
                }
            } else if (predicate instanceof OrJanusPredicate) {
                final List<Object> values = (List<Object>) (value);
                // FIXME: this might generate a non QNF-compliant form, e.g. nested = PredicateCondition OR (PredicateCondition AND PredicateCondition)
                final Or<E> nested = addConstraint(type, (OrJanusPredicate) predicate, values, new Or<>(values.size()), tx);
                if (nested == null) {
                    return null;
                }
                conditions.add(nested);
            } else {
                addConstraint(type, predicate, value, conditions, tx);
            }
        }
        return conditions;
    }

    private static <E extends JanusGraphElement> And<E> addConstraint(final RelationType type, AndJanusPredicate predicate, List<Object> values, And<E> and, StandardJanusGraphTx tx) {
        for (int i = 0 ; i < values.size(); i++) {
            final JanusGraphPredicate janusGraphPredicate = predicate.get(i);
            if (janusGraphPredicate instanceof Contain) {
                //Rewrite contains conditions
                final Collection childValues = (Collection) values.get(i);
                if (janusGraphPredicate == Contain.NOT_IN) {
                    if (childValues.isEmpty()) continue; //Simply ignore since trivially satisfied
                    for (final Object inValue : childValues)
                        addConstraint(type, Cmp.NOT_EQUAL, inValue, and, tx);
                } else {
                    Preconditions.checkArgument(janusGraphPredicate == Contain.IN);
                    if (childValues.isEmpty()) {
                        return null; //Cannot be satisfied
                    }
                    if (childValues.size() == 1) {
                        addConstraint(type, Cmp.EQUAL, childValues.iterator().next(), and, tx);
                    } else {
                        final Or<E> nested = new Or<>(childValues.size());
                        for (final Object inValue : childValues)
                            addConstraint(type, Cmp.EQUAL, inValue, nested, tx);
                        and.add(nested);
                    }
                }
            } else if (janusGraphPredicate instanceof AndJanusPredicate) {
                if (addConstraint(type, (AndJanusPredicate) (janusGraphPredicate), (List<Object>) (values.get(i)), and, tx) == null) {
                    return null;
                }
            } else if (predicate.get(i) instanceof OrJanusPredicate) {
                final List<Object> childValues = (List<Object>) (values.get(i));
                final Or<E> nested = addConstraint(type, (OrJanusPredicate) (janusGraphPredicate), childValues, new Or<>(childValues.size()), tx);
                if (nested == null) {
                    return null;
                }
                and.add(nested);
            } else {
                addConstraint(type, janusGraphPredicate, values.get(i), and, tx);
            }
        }
        return and;
    }

    private static <E extends JanusGraphElement> Or<E> addConstraint(final RelationType type, OrJanusPredicate predicate, List<Object> values, Or<E> or, StandardJanusGraphTx tx) {
        for (int i = 0 ; i < values.size(); i++) {
            final JanusGraphPredicate janusGraphPredicate = predicate.get(i);
            if (janusGraphPredicate instanceof Contain) {
                //Rewrite contains conditions
                final Collection childValues = (Collection) values.get(i);
                if (janusGraphPredicate == Contain.NOT_IN) {
                    if (childValues.size() == 1) {
                        addConstraint(type, Cmp.NOT_EQUAL, childValues.iterator().next(), or, tx);
                    }
                    // Don't need to handle the case where childValues is empty, because it defaults to
                    // an or(and()) is added, which is a tautology
                    final And<E> nested = new And<>(childValues.size());
                    for (final Object inValue : childValues) {
                        addConstraint(type, Cmp.NOT_EQUAL, inValue, nested, tx);
                    }
                    or.add(nested);
                } else {
                    Preconditions.checkArgument(janusGraphPredicate == Contain.IN);
                    if (childValues.isEmpty()) {
                        continue; // Handle any unsatisfiable condition that occurs within an OR statement like it does not exist
                    }
                    for (final Object inValue : childValues) {
                        addConstraint(type, Cmp.EQUAL, inValue, or, tx);
                    }
                }
            } else if (janusGraphPredicate instanceof AndJanusPredicate) {
                final List<Object> childValues = (List<Object>) (values.get(i));
                final And<E> nested = addConstraint(type, (AndJanusPredicate) janusGraphPredicate, childValues, new And<>(childValues.size()), tx);
                if (nested == null) {
                    return null;
                }
                or.add(nested);
            } else if (janusGraphPredicate instanceof OrJanusPredicate) {
                if (addConstraint(type, (OrJanusPredicate) janusGraphPredicate, (List<Object>) (values.get(i)), or, tx) == null) {
                    return null;
                }
            } else {
                addConstraint(type, janusGraphPredicate, values.get(i), or, tx);
            }
        }
        return or;
    }

    private static <E extends JanusGraphElement> void addConstraint(RelationType type, JanusGraphPredicate predicate,
                                                               Object value, MultiCondition<E> conditions, StandardJanusGraphTx tx) {
        if (type.isPropertyKey()) {
            if (value != null)
                value = tx.verifyAttribute((PropertyKey) type, value);
        } else { //t.isEdgeLabel()
            Preconditions.checkArgument(value instanceof JanusGraphVertex);
        }
        final PredicateCondition<RelationType, E> pc = new PredicateCondition<>(type, predicate, value);
        if (!conditions.contains(pc)) conditions.add(pc);
    }


    public static Map.Entry<RelationType,Collection> extractOrCondition(Or<JanusGraphRelation> condition) {
        RelationType masterType = null;
        final List<Object> values = new ArrayList<>();
        for (final Condition c : condition.getChildren()) {
            if (!(c instanceof PredicateCondition)) return null;
            final PredicateCondition<RelationType, JanusGraphRelation> atom = (PredicateCondition)c;
            if (atom.getPredicate()!=Cmp.EQUAL) return null;
            final Object value = atom.getValue();
            if (value==null) return null;
            final RelationType type = atom.getKey();
            if (masterType==null) masterType=type;
            else if (!masterType.equals(type)) return null;
            values.add(value);
        }
        if (masterType==null) return null;
        assert !values.isEmpty();
        return new AbstractMap.SimpleImmutableEntry(masterType,values);
    }


    public static <R> List<R> processIntersectingRetrievals(List<IndexCall<R>> retrievals, final int limit) {
        Preconditions.checkArgument(!retrievals.isEmpty());
        Preconditions.checkArgument(limit >= 0, "Invalid limit: %s", limit);
        List<R> results;
        /*
         * Iterate over the clauses in the and collection
         * query.getCondition().getChildren(), taking the intersection
         * of current results with cumulative results on each iteration.
         */
        //TODO: smarter limit estimation
        final int multiplier = Math.min(16, (int) Math.pow(2, retrievals.size() - 1));
        int subLimit = Integer.MAX_VALUE;
        if (Integer.MAX_VALUE / multiplier >= limit) subLimit = limit * multiplier;
        boolean exhaustedResults;
        do {
            exhaustedResults = true;
            results = null;
            for (final IndexCall<R> call : retrievals) {
                Collection<R> subResult;
                try {
                    subResult = call.call(subLimit);
                } catch (final Exception e) {
                    throw new JanusGraphException("Could not process individual retrieval call ", e);
                }

                if (subResult.size() >= subLimit) exhaustedResults = false;
                if (results == null) {
                    results = new ArrayList<>(subResult);
                } else {
                    final Set<R> subResultSet;
                    if(subResult instanceof Set){
                        subResultSet = (Set<R>) subResult;
                    } else {
                        subResultSet = new HashSet<>(subResult);
                    }
                    results.removeIf(o -> !subResultSet.contains(o));
                }
            }
            subLimit = (int) Math.min(Integer.MAX_VALUE - 1, Math.max(Math.pow(subLimit, 1.5),(subLimit+1)*2));
        } while (results != null && results.size() < limit && !exhaustedResults);
        return results;
    }


    public interface IndexCall<R> {

        Collection<R> call(int limit);

    }

}
