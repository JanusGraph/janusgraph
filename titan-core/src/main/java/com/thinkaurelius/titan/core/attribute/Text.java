package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comparison relations for text objects.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Text implements TitanPredicate {

    /**
     * Whether the text contains a given term
     */
    CONTAINS {

        private static final int MIN_TERM_LENGTH = 2;

        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            if (value == null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            String term = ((String) condition).trim().toLowerCase();
            if (term.length() < MIN_TERM_LENGTH) return false;
            String text = value.toString().toLowerCase();
            int position = 0;
            while (position >= 0) {
                position = text.indexOf(term, position);
                if (position >= 0) {
                    int afterPos = position + term.length();
//                    System.out.println(position + "|"+afterPos);
                    Preconditions.checkArgument(afterPos <= text.length());
                    if ((position == 0 || !Character.isLetterOrDigit(text.charAt(position - 1)))
                            && (afterPos == text.length() || !Character.isLetterOrDigit(text.charAt(afterPos)))) {
                        return true;
                    }
                    position = afterPos;
                }
            }
            return false;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            if (condition == null) return false;
            else if (condition instanceof String && StringUtils.isNotBlank((String) condition)) return true;
            else return false;
        }
    },

    /**
     * Whether the text starts with a given term
     */
    PREFIX {
        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof String);
            if (value == null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            return value.toString().startsWith((String) condition);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition != null && condition instanceof String;
        }

    };

    private static final Logger log = LoggerFactory.getLogger(Text.class);


    @Override
    public boolean isValidValueType(Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        return clazz.equals(String.class);
    }

    @Override
    public boolean hasNegation() {
        return false;
    }

    @Override
    public TitanPredicate negate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isQNF() {
        return true;
    }


}
