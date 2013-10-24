package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;

/**
 * Comparison relations for geographic shapes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Geo implements TitanPredicate {

    /**
     * Whether the intersection between two geographic regions is non-empty
     */
    INTERSECT {
        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value == null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape) value).intersect((Geoshape) condition);
        }

        @Override
        public String toString() {
            return "intersect";
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public TitanPredicate negate() {
            return DISJOINT;
        }
    },

    /**
     * Whether the intersection between two geographic regions is empty
     */
    DISJOINT {
        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value == null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape) value).disjoint((Geoshape) condition);
        }

        @Override
        public String toString() {
            return "disjoint";
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public TitanPredicate negate() {
            return INTERSECT;
        }
    },

    /**
     * Whether one geographic region is completely contains within another
     */
    WITHIN {
        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value == null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape) value).within((Geoshape) condition);
        }

        @Override
        public String toString() {
            return "within";
        }

        @Override
        public boolean hasNegation() {
            return false;
        }

        @Override
        public TitanPredicate negate() {
            throw new UnsupportedOperationException();
        }
    };


    @Override
    public boolean isValidCondition(Object condition) {
        return condition != null && condition instanceof Geoshape;
    }

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        return clazz.equals(Geoshape.class);
    }

    @Override
    public boolean isQNF() {
        return true;
    }

}
