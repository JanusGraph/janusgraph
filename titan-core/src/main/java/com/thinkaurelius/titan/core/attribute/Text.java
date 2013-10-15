package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Comparison relations for text objects. These comparisons are based on a tokenized representation
 * of the text, i.e. the text is considered as a set of word tokens.
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
            for (String token : tokenize(value.toString().toLowerCase())) {
                if (token.equals(term)) return true;
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
     * Whether the text contains a token that starts with a given term
     */
    PREFIX {
        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            if (value == null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            String prefix = ((String) condition).trim().toLowerCase();
            for (String token : tokenize(value.toString().toLowerCase())) {
                if (token.startsWith(prefix)) return true;
            }
            return false;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition != null && condition instanceof String;
        }

    },

    /**
     * Whether the text contains a token that matches a regular expression
     */
    REGEX {
        @Override
        public boolean evaluate(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition), "Invalid condition provided: %s", condition);
            if (value == null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            String regex = (String) condition;
            //TODO: compile regex for efficiency
            for (String token : tokenize(value.toString().toLowerCase())) {
                if (token.matches(regex)) return true;
            }
            return false;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition != null && condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

    };

    private static final Logger log = LoggerFactory.getLogger(Text.class);

    public static List<String> tokenize(String str) {
        ArrayList<String> tokens = new ArrayList<String>();
        int previous = 0;
        for (int p = 0; p < str.length(); p++) {
            if (!Character.isLetterOrDigit(str.charAt(p))) {
                if (p > previous) tokens.add(str.substring(previous, p));
                previous = p + 1;
            }
        }
        if (previous < str.length()) tokens.add(str.substring(previous, str.length()));
        return tokens;
    }

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
