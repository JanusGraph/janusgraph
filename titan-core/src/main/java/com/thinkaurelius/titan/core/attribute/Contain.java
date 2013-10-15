package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Comparison relations for text objects.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Contain implements TitanPredicate {

    /**
     * Whether an element is in a collection
     */
    IN {
        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            Collection col = (Collection) condition;
            return col.contains(value);
        }

        @Override
        public TitanPredicate negate() {
            return NOT_IN;
        }
    },

    /**
     * Whether an element is not in a collection
     */
    NOT_IN {
        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            Collection col = (Collection) condition;
            return !col.contains(value);
        }

        @Override
        public TitanPredicate negate() {
            return IN;
        }

    };

    private static final Logger log = LoggerFactory.getLogger(Contain.class);

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        return true;
    }

    @Override
    public boolean isValidCondition(Object condition) {
        return condition != null && (condition instanceof Collection) && !((Collection) condition).isEmpty();
    }

    @Override
    public boolean hasNegation() {
        return true;
    }

    @Override
    public boolean isQNF() {
        return false;
    }


}
