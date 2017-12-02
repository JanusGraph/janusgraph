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
import com.google.common.collect.*;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.query.condition.*;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import java.util.*;
import java.util.stream.StreamSupport;

/**
 * Utility methods used in query optimization and processing.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class QueryUtil {

    public static int adjustLimitForTxModifications(StandardJanusGraphTx tx, int uncoveredAndConditions, int limit) {
        assert limit > 0 && limit <= 1000000000; //To make sure limit computation does not overflow
        assert uncoveredAndConditions >= 0;

        if (uncoveredAndConditions > 0) {
            int maxMultiplier = Integer.MAX_VALUE / limit;
            limit = limit * Math.min(maxMultiplier, (int) Math.pow(2, uncoveredAndConditions)); //(limit*3)/2+1;
        }

        if (tx.hasModifications())
            limit += Math.min(Integer.MAX_VALUE - limit, 5);

        return limit;
    }

    public static int convertLimit(long limit) {
        assert limit>=0;
        if (limit>=Integer.MAX_VALUE) return Integer.MAX_VALUE;
        else return (int)limit;
    }

    public static int mergeLimits(int limit1, int limit2) {
        assert limit1>=0 && limit2>=0;
        return Math.min(limit1,limit2);
    }

    public static InternalRelationType getType(StandardJanusGraphTx tx, String typeName) {
        RelationType t = tx.getRelationType(typeName);
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
            if (isQNFLiteralOrNot(child)) {
                continue;
            } else if (child instanceof Or) {
                for (Condition<?> child2 : ((Or<?>) child).getChildren()) {
                    if (!isQNFLiteralOrNot(child2)) return false;
                }
            } else {
                return false;
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

    public static <E extends JanusGraphElement> Condition<E> simplifyQNF(Condition<E> condition) {
        Preconditions.checkArgument(isQueryNormalForm(condition));
        if (condition.numChildren() == 1) {
            Condition<E> child = ((And<E>) condition).get(0);
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
        And<E> conditions = new And<E>(constraints.size() + 4);
        for (PredicateCondition<String, E> atom : constraints) {
            RelationType type = getType(tx, atom.getKey());

            if (type == null) {
                if (atom.getPredicate() == Cmp.EQUAL && atom.getValue() == null ||
                        (atom.getPredicate() == Cmp.NOT_EQUAL && atom.getValue() != null))
                    continue; //Ignore condition, its trivially satisfied

                return null;
            }

            Object value = atom.getValue();
            JanusGraphPredicate predicate = atom.getPredicate();


            if (type.isPropertyKey()) {
                PropertyKey key = (PropertyKey) type;
                assert predicate.isValidCondition(value);
                Preconditions.checkArgument(key.dataType()==Object.class || predicate.isValidValueType(key.dataType()), "Data type of key is not compatible with condition");
            } else { //its a label
                Preconditions.checkArgument(((EdgeLabel) type).isUnidirected());
                Preconditions.checkArgument(predicate.isValidValueType(JanusGraphVertex.class), "Data type of key is not compatible with condition");
            }

            if (predicate instanceof Contain) {
                //Rewrite contains conditions
                Collection values = (Collection) value;
                if (predicate == Contain.NOT_IN) {
                    if (values.isEmpty()) continue; //Simply ignore since trivially satisfied
                    for (Object inValue : values)
                        addConstraint(type, Cmp.NOT_EQUAL, inValue, conditions, tx);
                } else {
                    Preconditions.checkArgument(predicate == Contain.IN);
                    if (values.isEmpty()) {
                        return null; //Cannot be satisfied
                    } if (values.size() == 1) {
                        addConstraint(type, Cmp.EQUAL, values.iterator().next(), conditions, tx);
                    } else {
                        Or<E> nested = new Or<E>(values.size());
                        for (Object invalue : values)
                            addConstraint(type, Cmp.EQUAL, invalue, nested, tx);
                        conditions.add(nested);
                    }
                }
            } else {
                addConstraint(type, predicate, value, conditions, tx);
            }
        }
        return conditions;
    }

    private static <E extends JanusGraphElement> void addConstraint(RelationType type, JanusGraphPredicate predicate,
                                                               Object value, MultiCondition<E> conditions, StandardJanusGraphTx tx) {
        if (type.isPropertyKey()) {
            if (value != null)
                value = tx.verifyAttribute((PropertyKey) type, value);
        } else { //t.isEdgeLabel()
            Preconditions.checkArgument(value instanceof JanusGraphVertex);
        }
        PredicateCondition<RelationType, E> pc = new PredicateCondition<RelationType, E>(type, predicate, value);
        if (!conditions.contains(pc)) conditions.add(pc);
    }


    public static Map.Entry<RelationType,Collection> extractOrCondition(Or<JanusGraphRelation> condition) {
        RelationType masterType = null;
        List<Object> values = new ArrayList<Object>();
        for (Condition c : condition.getChildren()) {
            if (!(c instanceof PredicateCondition)) return null;
            PredicateCondition<RelationType, JanusGraphRelation> atom = (PredicateCondition)c;
            if (atom.getPredicate()!=Cmp.EQUAL) return null;
            Object value = atom.getValue();
            if (value==null) return null;
            RelationType type = atom.getKey();
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
        int multiplier = Math.min(16, (int) Math.pow(2, retrievals.size() - 1));
        int subLimit = Integer.MAX_VALUE;
        if (Integer.MAX_VALUE / multiplier >= limit) subLimit = limit * multiplier;
        boolean exhaustedResults;
        do {
            exhaustedResults = true;
            results = null;
            for (IndexCall<R> call : retrievals) {
                Collection<R> subResult;
                try {
                    subResult = call.call(subLimit);
                } catch (Exception e) {
                    throw new JanusGraphException("Could not process individual retrieval call ", e);
                }

                if (subResult.size() >= subLimit) exhaustedResults = false;
                if (results == null) {
                    results = Lists.newArrayList(subResult);
                } else {
                    Set<R> subResultSet = ImmutableSet.copyOf(subResult);
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
