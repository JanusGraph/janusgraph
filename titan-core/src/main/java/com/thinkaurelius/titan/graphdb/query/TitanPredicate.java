package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Query;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface TitanPredicate extends Predicate {

    public boolean isValidCondition(Object condition);

    public boolean isValidValueType(Class<?> clazz);

    public boolean hasNegation();

    public TitanPredicate negate();

    public boolean isQNF();

    @Override
    public boolean evaluate(Object value, Object condition);


    public static class Converter {

        /**
         * Convert Blueprint's comparison operators to Titan's
         *
         * @param p Any predicate
         * @return A TitanPredicate equivalent to the given predicate
         * @throws IllegalArgumentException if the given Predicate is unknown
         */
        public static final TitanPredicate convert(Predicate p) {
            if (p instanceof TitanPredicate) return (TitanPredicate)p;
            else if (p instanceof Query.Compare) {
                Query.Compare comp = (Query.Compare)p;
                switch(comp) {
                    case EQUAL: return Cmp.EQUAL;
                    case NOT_EQUAL: return Cmp.NOT_EQUAL;
                    case GREATER_THAN: return Cmp.GREATER_THAN;
                    case GREATER_THAN_EQUAL: return Cmp.GREATER_THAN_EQUAL;
                    case LESS_THAN: return Cmp.LESS_THAN;
                    case LESS_THAN_EQUAL: return Cmp.LESS_THAN_EQUAL;
                    default: throw new IllegalArgumentException("Unexpected comparator: " + comp);
                }
            } else if (p instanceof Compare) {
                Compare comp = (Compare)p;
                switch(comp) {
                    case EQUAL: return Cmp.EQUAL;
                    case NOT_EQUAL: return Cmp.NOT_EQUAL;
                    case GREATER_THAN: return Cmp.GREATER_THAN;
                    case GREATER_THAN_EQUAL: return Cmp.GREATER_THAN_EQUAL;
                    case LESS_THAN: return Cmp.LESS_THAN;
                    case LESS_THAN_EQUAL: return Cmp.LESS_THAN_EQUAL;
                    default: throw new IllegalArgumentException("Unexpected comparator: " + comp);
                }
            } else if (p instanceof Contains) {
                Contains con = (Contains)p;
                switch (con) {
                    case IN: return Contain.IN;
                    case NOT_IN: return Contain.NOT_IN;
                    default: throw new IllegalArgumentException("Unexpected container: " + con);

                }
            } else throw new IllegalArgumentException("Titan does not support the given predicate: " + p);
        }

    }

}
