package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.diskstorage.indexing.IndexInformation;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;

public class QueryUtil {

    public static final TitanProperty queryHiddenUniqueProperty(InternalVertex vertex, TitanKey key) {
        assert ((InternalType) key).isHidden() : "Expected hidden property key";
        assert key.isUnique(Direction.OUT) : "Expected functional property  type";
        return Iterables.getOnlyElement(
                vertex.query().
                        includeHidden().
                        type(key).
                        properties(), null);
    }

    public static final Iterable<TitanRelation> queryAll(InternalVertex vertex) {
        return vertex.query().includeHidden().relations();
    }

    public static final int adjustLimitForTxModifications(StandardTitanTx tx, int uncoveredAndConditions, int limit) {
        Preconditions.checkArgument(limit > 0 && limit <= 1000000000, "Invalid limit: %s", limit); //To make sure limit computation does not overflow
        Preconditions.checkArgument(uncoveredAndConditions >= 0);
        if (uncoveredAndConditions > 0) {
            int maxMultiplier = Integer.MAX_VALUE / limit;
            limit = limit * Math.min(maxMultiplier, (int) Math.pow(2, uncoveredAndConditions)); //(limit*3)/2+1;
        }
        if (tx.hasModifications()) limit += Math.min(Integer.MAX_VALUE - limit, 5);
        return limit;
    }

    private static final InternalType getType(StandardTitanTx tx, String typeName) {
        TitanType t = tx.getType(typeName);
        if (t == null && !tx.getConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return (InternalType) t;
    }

    /**
     * Query-normal-form (QNF) for Titan is a variant of CNF (conjunctive normal form) with negation inlined where possible
     *
     * @param condition
     * @return
     */
    public static final boolean isQueryNormalForm(Condition<?> condition) {
        if (isQNFLiteralOrNot(condition)) return true;
        else if (condition instanceof And) {
            for (Condition<?> child : ((And<?>) condition).getChildren()) {
                if (isQNFLiteralOrNot(child)) continue;
                else if (child instanceof Or) {
                    for (Condition<?> child2 : ((Or<?>) child).getChildren()) {
                        if (!isQNFLiteralOrNot(child2)) return false;
                    }
                } else return false;
            }
            return true;
        } else return false;
    }

    private static final boolean isQNFLiteralOrNot(Condition<?> condition) {
        if (condition instanceof Not) {
            Condition child = ((Not) condition).getChild();
            if (!isQNFLiteral(child)) return false;
            else if (child instanceof PredicateCondition) {
                return !((PredicateCondition) child).getPredicate().hasNegation();
            } else return true;
        } else return isQNFLiteral(condition);
    }

    private static final boolean isQNFLiteral(Condition<?> condition) {
        if (condition.getType() != Condition.Type.LITERAL) return false;
        if (condition instanceof PredicateCondition) {
            return ((PredicateCondition) condition).getPredicate().isQNF();
        } else return true;
    }

    private static final <E extends TitanElement> Condition<E> inlineNegation(Condition<E> condition) {
        if (ConditionUtil.containsType(condition, Condition.Type.NOT)) {
            return ConditionUtil.transformation(condition, new Function<Condition<E>, Condition<E>>() {
                @Nullable
                @Override
                public Condition<E> apply(@Nullable Condition<E> cond) {
                    if (cond instanceof Not) {
                        Condition<E> child = ((Not) cond).getChild();
                        Preconditions.checkArgument(child.getType() == Condition.Type.LITERAL); //verify QNF
                        if (child instanceof PredicateCondition) {
                            PredicateCondition<?, E> pc = (PredicateCondition) child;
                            if (pc.getPredicate().hasNegation()) {
                                return new PredicateCondition(pc.getKey(), pc.getPredicate().negate(), pc.getValue());
                            }
                        }
                    }
                    return null;
                }
            });
        } else return condition;
    }

    public static final <E extends TitanElement> Condition<E> simplifyQNF(Condition<E> condition) {
        Preconditions.checkArgument(isQueryNormalForm(condition));
        if (condition.numChildren() == 1) {
            Condition<E> child = ((And) condition).get(0);
            if (child.getType() == Condition.Type.LITERAL) return child;
        }
        return condition;
    }

    public static final boolean isEmpty(Condition<?> condition) {
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
     * @see #isQueryNormalForm(com.thinkaurelius.titan.graphdb.query.condition.Condition)
     */
    public static final <E extends TitanElement> And<E> constraints2QNF(StandardTitanTx tx, List<PredicateCondition<String, E>> constraints) {
        And<E> conditions = new And<E>(constraints.size() + 4);
        for (int i = 0; i < constraints.size(); i++) {
            PredicateCondition<String, E> atom = constraints.get(i);
            TitanType type = getType(tx, atom.getKey());
            if (type == null) {
                if (atom.getPredicate() == Cmp.EQUAL && atom.getValue() == null)
                    continue; //Ignore condition, its trivially satisifed
                else return null;
            }
            Object value = atom.getValue();
            TitanPredicate predicate = atom.getPredicate();
            Preconditions.checkArgument(predicate.isValidCondition(value), "Invalid condition: %s", value);
            if (type.isPropertyKey()) {
                Preconditions.checkArgument(predicate.isValidValueType(((TitanKey) type).getDataType()), "Data type of key is not compatible with condition");
            } else { //its a label
                Preconditions.checkArgument(((TitanLabel) type).isUnidirected());
                Preconditions.checkArgument(predicate.isValidValueType(TitanVertex.class), "Data type of key is not compatible with condition");
            }

            if (predicate instanceof Contain) {
                //Rewrite contains conditions
                Collection values = (Collection) value;
                if (predicate == Contain.NOT_IN) {
                    for (Object invalue : values) addConstraint(type, Cmp.NOT_EQUAL, invalue, conditions, tx);
                } else {
                    Preconditions.checkArgument(predicate == Contain.IN);
                    if (values.size() == 1) addConstraint(type, Cmp.EQUAL, values.iterator().next(), conditions, tx);
                    else {
                        Or<E> nested = new Or<E>(values.size());
                        for (Object invalue : values) addConstraint(type, Cmp.EQUAL, invalue, nested, tx);
                        conditions.add(nested);
                    }
                }
            } else {
                addConstraint(type, predicate, value, conditions, tx);
            }
        }
        return conditions;
    }

    private static final <E extends TitanElement> void addConstraint(TitanType type, TitanPredicate predicate,
                                                                     Object value, MultiCondition<E> conditions, StandardTitanTx tx) {
        if (type.isPropertyKey()) {
            if (value != null) value = tx.verifyAttribute((TitanKey) type, value);
        } else { //t.isEdgeLabel()
            Preconditions.checkArgument(value instanceof TitanVertex);
        }
        conditions.add(new PredicateCondition<TitanType, E>(type, predicate, value));
    }

    private static final Set<String> NO_INDEXES = ImmutableSet.of();

    /**
     * Returns the names of the indexes that cover this condition (i.e. can return the result set for this condition).
     * It is assumed that the given condition is from the top level AND clause of a QNF formula.
     *
     * @param result
     * @param condition
     * @return
     */
    public static final Set<String> andClauseIndexCover(final ElementType result, Condition<TitanElement> condition, IndexSerializer indexInfo) {
        Set<String> indexes = NO_INDEXES;
        if (condition instanceof PredicateCondition) {
            PredicateCondition<TitanType, TitanElement> atom = (PredicateCondition) condition;
            if (atom.getValue() != null) {
                Preconditions.checkArgument(atom.getKey().isPropertyKey());
                TitanKey key = (TitanKey) atom.getKey();
                indexes = Sets.newHashSet(key.getIndexes(result.getElementType()));
                Iterator<String> indexiter = indexes.iterator();
                while (indexiter.hasNext()) {
                    if (!indexInfo.getIndexInformation(indexiter.next()).supports(key.getDataType(), atom.getPredicate())) {
                        indexiter.remove();
                    }
                }

            } else {
                //Not supported
            }
        } else if (condition instanceof Not) {
            return andClauseIndexCover(result, ((Not<TitanElement>) condition).getChild(), indexInfo);
        } else if (condition instanceof Or) {
            boolean matchesAll = true;
            for (Condition<TitanElement> child : condition.getChildren()) {
                Set<String> subindexes = andClauseIndexCover(result, child, indexInfo);
                if (indexes == NO_INDEXES) indexes = subindexes;
                else indexes.retainAll(subindexes);
            }
        } else throw new IllegalArgumentException("Query not in QNF: " + condition);
        return indexes;
    }


    public static <R> List<R> processIntersectingRetrievals(List<IndexCall<R>> retrievals, final int limit) {
        Preconditions.checkArgument(!retrievals.isEmpty());
        Preconditions.checkArgument(limit >= 0, "Invalid limit: %s", limit);
        List<R> results = null;
        /*
         * Iterate over the clauses in the and collection
         * query.getCondition().getChildren(), taking the intersection
         * of current results with cumulative results on each iteration.
         */
        //TODO: smarter limit estimation
        int multiplier = Math.min(16, (int) Math.pow(2, retrievals.size() - 1));
        int sublimit = Integer.MAX_VALUE;
        if (Integer.MAX_VALUE / multiplier >= limit) sublimit = limit * multiplier;
        boolean exhaustedResults;
        do {
            exhaustedResults = true;
            results = null;
            for (IndexCall<R> call : retrievals) {
                Collection<R> subresult;
                try {
                    subresult = call.call(sublimit);
                } catch (Exception e) {
                    throw new TitanException("Could not process individual retrieval call ", e);
                }

                if (subresult.size() >= sublimit) exhaustedResults = false;
                if (results == null) {
                    results = Lists.newArrayList(subresult);
                } else {
                    Set<R> subresultset = ImmutableSet.copyOf(subresult);
                    Iterator riter = results.iterator();
                    while (riter.hasNext()) {
                        if (!subresultset.contains(riter.next())) riter.remove();
                    }
                }
            }
            sublimit = (int) Math.min(Integer.MAX_VALUE - 1, Math.pow(sublimit, 1.5));
        } while (results.size() < limit && !exhaustedResults);
        return results;
    }


    public interface IndexCall<R> {

        public Collection<R> call(int limit);

    }

}
