package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public enum Txt implements Relation {

    //TODO: generalize to also allow String[] as condition
    CONTAINS {

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof String);
            if (value==null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            return value.toString().contains((String)condition);
        }
    },

    PREFIX {

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof String);
            if (value==null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            return value.toString().startsWith((String)condition);
        }
    };

    private static final Logger log = LoggerFactory.getLogger(Txt.class);

    @Override
    public boolean isValidCondition(Object condition) {
        return condition!=null && condition instanceof String;
    }

    @Override
    public boolean isValidDataType(Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        return clazz.equals(Object.class) || clazz.equals(String.class);
    }


}
