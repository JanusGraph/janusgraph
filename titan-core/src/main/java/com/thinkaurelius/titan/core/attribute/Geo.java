package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;

/**
 * Comparison relations for geographic shapes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Geo implements Relation {

    INTERSECT {

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value==null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape)value).intersect((Geoshape)condition);
        }

        @Override
        public String toString() {
            return "intersect";
        }
    },

    DISJOINT {


        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value==null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape)value).disjoint((Geoshape)condition);
        }

        @Override
        public String toString() {
            return "disjoint";
        }
    },

    WITHIN {

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof Geoshape);
            if (value==null) return false;
            Preconditions.checkArgument(value instanceof Geoshape);
            return ((Geoshape)value).within((Geoshape)condition);
        }

        @Override
        public String toString() {
            return "within";
        }
    };


    @Override
    public boolean isValidCondition(Object condition) {
        return condition!=null && condition instanceof Geoshape;
    }

    @Override
    public boolean isValidDataType(Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        return clazz.equals(Geoshape.class);
    }
}
