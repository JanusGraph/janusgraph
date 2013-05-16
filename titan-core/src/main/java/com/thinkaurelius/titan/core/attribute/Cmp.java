package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import com.tinkerpop.blueprints.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic comparison relations for comparable (i.e. linearly ordered) objects.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Cmp implements Relation {

    EQUAL {

        @Override
        public boolean isValidDataType(Class<?> clazz) {
            return true;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return true;
        }

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
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
    },

    NOT_EQUAL {

        @Override
        public boolean isValidDataType(Class<?> clazz) {
            return true;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return true;
        }

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
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
    },

    LESS_THAN {

        @Override
        public boolean isValidDataType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Comparable;
        }

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
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
    },

    LESS_THAN_EQUAL {

        @Override
        public boolean isValidDataType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Comparable;
        }

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
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
    },

    GREATER_THAN {

        @Override
        public boolean isValidDataType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Comparable;
        }

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
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
    },

    GREATER_THAN_EQUAL {

        @Override
        public boolean isValidDataType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Comparable;
        }

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
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
    },

    INTERVAL {

        @Override
        public boolean isValidDataType(Class<?> clazz) {
            Preconditions.checkNotNull(clazz);
            return Comparable.class.isAssignableFrom(clazz);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof Interval;
        }

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            if (value==null) return false;
            Preconditions.checkArgument(condition instanceof Interval);
            try {
                return ((Interval)condition).inInterval(value);
            } catch (Throwable e) {
                log.warn("Could not compare element: {} - {}",value,condition);
                return false;
            }
        }

        @Override
        public String toString() {
            return "in";
        }
    };


    private static final Logger log = LoggerFactory.getLogger(Cmp.class);


    /**
     * Convert Blueprint's comparison operators to Titan's
     *
     * @param comp Blueprint's comparison operator
     * @return
     */
    public static final Cmp convert(Query.Compare comp) {
        switch(comp) {
            case EQUAL: return EQUAL;
            case NOT_EQUAL: return NOT_EQUAL;
            case GREATER_THAN: return GREATER_THAN;
            case GREATER_THAN_EQUAL: return GREATER_THAN_EQUAL;
            case LESS_THAN: return LESS_THAN;
            case LESS_THAN_EQUAL: return LESS_THAN_EQUAL;
            default: throw new IllegalArgumentException("Unexpected comparator: " + comp);
        }
    }

}
