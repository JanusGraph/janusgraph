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

package org.janusgraph.core.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.graphdb.database.serialize.AttributeUtil;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.apache.commons.lang.ArrayUtils;

/**
 * Basic comparison relations for comparable (i.e. linearly ordered) objects.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Cmp implements JanusGraphPredicate {

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
        public boolean test(Object value, Object condition) {
            if (condition==null) {
                return value==null;
            } else {
                return condition.equals(value) || (condition.getClass().isArray() && ArrayUtils.isEquals(condition, value));
            }
        }

        @Override
        public String toString() {
            return "=";
        }

        @Override
        public JanusGraphPredicate negate() {
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
        public boolean test(Object value, Object condition) {
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
        public JanusGraphPredicate negate() {
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
        public boolean test(Object value, Object condition) {
            Integer cmp = AttributeUtil.compare(value,condition);
            return cmp != null && cmp < 0;
        }

        @Override
        public String toString() {
            return "<";
        }

        @Override
        public JanusGraphPredicate negate() {
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
        public boolean test(Object value, Object condition) {
            Integer cmp = AttributeUtil.compare(value,condition);
            return cmp != null && cmp <= 0;
        }

        @Override
        public String toString() {
            return "<=";
        }

        @Override
        public JanusGraphPredicate negate() {
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
        public boolean test(Object value, Object condition) {
            Integer cmp = AttributeUtil.compare(value,condition);
            return cmp != null && cmp > 0;
        }

        @Override
        public String toString() {
            return ">";
        }

        @Override
        public JanusGraphPredicate negate() {
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
        public boolean test(Object value, Object condition) {
            Integer cmp = AttributeUtil.compare(value,condition);
            return cmp != null && cmp >= 0;
        }

        @Override
        public String toString() {
            return ">=";
        }

        @Override
        public JanusGraphPredicate negate() {
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

}
