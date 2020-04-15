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
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;

/**
 * Comparison relations for geographic shapes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Geo implements JanusGraphPredicate {

    /**
     * Whether the intersection between two geographic regions is non-empty
     */
    INTERSECT {
        @Override
        public boolean test(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value == null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape) value).intersect((Geoshape) condition);
        }

        @Override
        public String toString() {
            return "geoIntersect";
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return DISJOINT;
        }
    },

    /**
     * Whether the intersection between two geographic regions is empty
     */
    DISJOINT {
        @Override
        public boolean test(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value == null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape) value).disjoint((Geoshape) condition);
        }

        @Override
        public String toString() {
            return "geoDisjoint";
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return INTERSECT;
        }
    },

    /**
     * Whether one geographic region is completely within another
     */
    WITHIN {
        @Override
        public boolean test(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value == null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape) value).within((Geoshape) condition);
        }

        @Override
        public String toString() {
            return "geoWithin";
        }

        @Override
        public boolean hasNegation() {
            return false;
        }

        @Override
        public JanusGraphPredicate negate() {
            throw new UnsupportedOperationException();
        }
    },

    /**
     * Whether one geographic region completely contains another
     */
    CONTAINS {
        @Override
        public boolean test(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value == null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape) value).contains((Geoshape) condition);
        }

        @Override
        public String toString() {
            return "geoContains";
        }

        @Override
        public boolean hasNegation() {
            return false;
        }

        @Override
        public JanusGraphPredicate negate() {
            throw new UnsupportedOperationException();
        }
    };

    @Override
    public boolean isValidCondition(Object condition) {
        return condition instanceof Geoshape;
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

    //////////////// statics

    public static <V> JanusGraphP geoIntersect(final V value) {
        return new JanusGraphP(Geo.INTERSECT, value);
    }
    public static <V> JanusGraphP geoDisjoint(final V value) {
        return new JanusGraphP(Geo.DISJOINT, value);
    }
    public static <V> JanusGraphP geoWithin(final V value) {
        return new JanusGraphP(Geo.WITHIN, value);
    }
    public static <V> JanusGraphP geoContains(final V value) {
        return new JanusGraphP(Geo.CONTAINS, value);
    }
}
