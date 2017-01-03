package org.janusgraph.graphdb.query;

import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;

import java.util.function.BiPredicate;

/**
 * A special kind of {@link BiPredicate} which marks all the predicates that are natively supported by
 * Janus and known to the query optimizer. Contains some custom methods that Janus needs for
 * query answering and evaluation.
 * </p>
 * This class contains a subclass used to convert Tinkerpop's {@link BiPredicate} implementations to the corresponding Janus predicates.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusPredicate extends BiPredicate<Object, Object> {

    /**
     * Whether the given condition is a valid condition for this predicate.
     * </p>
     * For instance, the {@link Cmp#GREATER_THAN} would require that the condition is comparable and not null.
     *
     * @param condition
     * @return
     */
    public boolean isValidCondition(Object condition);

    /**
     * Whether the given class is a valid data type for a value to which this predicate may be applied.
     * </p>
     * For instance, the {@link Cmp#GREATER_THAN} can only be applied to {@link Comparable} values.
     *
     * @param clazz
     * @return
     */
    public boolean isValidValueType(Class<?> clazz);

    /**
     * Whether this predicate has a predicate that is semantically its negation.
     * For instance, {@link Cmp#EQUAL} and {@link Cmp#NOT_EQUAL} are negatives of each other.
     *
     * @return
     */
    public boolean hasNegation();

    /**
     * Returns the negation of this predicate if it exists, otherwise an exception is thrown. Check {@link #hasNegation()} first.
     * @return
     */
    public JanusPredicate negate();

    /**
     * Returns true if this predicate is in query normal form.
     * @return
     */
    public boolean isQNF();


    @Override
    public boolean test(Object value, Object condition);


    public static class Converter {

        /**
         * Convert Tinkerpop's comparison operators to Janus's
         *
         * @param p Any predicate
         * @return A JanusPredicate equivalent to the given predicate
         * @throws IllegalArgumentException if the given Predicate is unknown
         */
        public static final JanusPredicate convertInternal(BiPredicate p) {
            if (p instanceof JanusPredicate) {
                return (JanusPredicate)p;
            } else if (p instanceof Compare) {
                Compare comp = (Compare)p;
                switch(comp) {
                    case eq: return Cmp.EQUAL;
                    case neq: return Cmp.NOT_EQUAL;
                    case gt: return Cmp.GREATER_THAN;
                    case gte: return Cmp.GREATER_THAN_EQUAL;
                    case lt: return Cmp.LESS_THAN;
                    case lte: return Cmp.LESS_THAN_EQUAL;
                    default: throw new IllegalArgumentException("Unexpected comparator: " + comp);
                }
            } else if (p instanceof Contains) {
                Contains con = (Contains)p;
                switch (con) {
                    case within: return Contain.IN;
                    case without: return Contain.NOT_IN;
                    default: throw new IllegalArgumentException("Unexpected container: " + con);

                }
            } else return null;
        }

        public static final JanusPredicate convert(BiPredicate p) {
            JanusPredicate janusPred = convertInternal(p);
            if (janusPred==null) throw new IllegalArgumentException("Janus does not support the given predicate: " + p);
            return janusPred;
        }

        public static final boolean supports(BiPredicate p) {
            return convertInternal(p)!=null;
        }
    }

}
