package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public enum Cmd implements Relation {

    EQUALS {

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

    LESS_THAN_EQUALS {

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

    GREATER_THAN_EQUALS {

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


    private static final Logger log = LoggerFactory.getLogger(Cmd.class);

}
