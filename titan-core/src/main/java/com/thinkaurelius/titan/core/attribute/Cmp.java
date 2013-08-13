package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic comparison relations for comparable (i.e. linearly ordered) objects.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Cmp implements TitanPredicate {

    EQUAL {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            return true;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return true;
        }

        @Override
        public boolean evaluate(Object value, Object condition) {
            if (condition==null) {
                return value==null;
            } else {
                return condition.equals(value);
            }
        }

        @Override
        public String toString() {
            return "=";
        }

        @Override
        public TitanPredicate negate() {
            return NOT_EQUAL;
        }
    },

    NOT_EQUAL {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            return true;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return true;
        }

        @Override
        public boolean evaluate(Object value, Object condition) {
            if (condition==null) {
                return value!=null;
            } else {
                return !condition.equals(value);
            }
        }

        @Override
        public String toString() {
            return "<>";
        }

        @Override
        public TitanPredicate negate() {
            return EQUAL;
        }
    },

    LESS_THAN {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Comparable;
        }

        @Override
        public boolean evaluate(Object value, Object condition) {
            if (value==null) return false;
            try {
                return ((Comparable)value).compareTo(condition)<0;
            } catch (Throwable e) {
                log.warn("Could not compare element: {} - {}",value,condition);
                return false;
            }
        }

        @Override
        public String toString() {
            return "<";
        }

        @Override
        public TitanPredicate negate() {
            return GREATER_THAN_EQUAL;
        }
    },

    LESS_THAN_EQUAL {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Comparable;
        }

        @Override
        public boolean evaluate(Object value, Object condition) {
            if (value==null) return false;
            try {
                return ((Comparable)value).compareTo(condition)<=0;
            } catch (Throwable e) {
                log.warn("Could not compare element: {} - {}",value,condition);
                return false;
            }
        }

        @Override
        public String toString() {
            return "<=";
        }

        @Override
        public TitanPredicate negate() {
            return GREATER_THAN;
        }
    },

    GREATER_THAN {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Comparable;
        }

        @Override
        public boolean evaluate(Object value, Object condition) {
            if (value==null) return false;
            try {
                return ((Comparable)value).compareTo(condition)>0;
            } catch (Throwable e) {
                log.warn("Could not compare element: {} - {}",value,condition);
                return false;
            }
        }

        @Override
        public String toString() {
            return ">";
        }

        @Override
        public TitanPredicate negate() {
            return LESS_THAN_EQUAL;
        }
    },

    GREATER_THAN_EQUAL {

        @Override
        public boolean isValidValueType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Comparable;
        }

        @Override
        public boolean evaluate(Object value, Object condition) {
            if (value==null) return false;
            try {
                return ((Comparable)value).compareTo(condition)>=0;
            } catch (Throwable e) {
                log.warn("Could not compare element: {} - {}",value,condition);
                return false;
            }
        }

        @Override
        public String toString() {
            return ">=";
        }

        @Override
        public TitanPredicate negate() {
            return LESS_THAN;
        }
    };


    @Override
    public boolean hasNegation() {
        return true;
    }

    @Override
    public boolean isQNF() {
        return true;
    }

    private static final Logger log = LoggerFactory.getLogger(Cmp.class);

}
