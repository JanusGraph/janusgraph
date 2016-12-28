package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Comparison relations for text objects. These comparisons are based on a tokenized representation
 * of the text, i.e. the text is considered as a set of word tokens.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Text implements TitanPredicate {

    /**
     * Whether the text contains a given term as a token in the text (case insensitive)
     */
    CONTAINS {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value,condition);
            if (value == null) return false;
            return evaluateRaw(value.toString(),(String)condition);
        }

        @Override
        public boolean evaluateRaw(String value, String terms) {
            Set<String> tokens = Sets.newHashSet(tokenize(value.toLowerCase()));
            terms = terms.trim();
            List<String> tokenTerms = tokenize(terms.toLowerCase());
            if (!terms.isEmpty() && tokenTerms.isEmpty()) return false;
            for (String term : tokenTerms) {
                if (!tokens.contains(term)) return false;
            }
            return true;
        }


        @Override
        public boolean isValidCondition(Object condition) {
            if (condition == null) return false;
            else if (condition instanceof String && StringUtils.isNotBlank((String) condition)) return true;
            else return false;
        }
    },

    /**
     * Whether the text contains a token that starts with a given term (case insensitive)
     */
    CONTAINS_PREFIX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value,condition);
            if (value == null) return false;
            return evaluateRaw(value.toString(),(String)condition);
        }

        @Override
        public boolean evaluateRaw(String value, String prefix) {
            for (String token : tokenize(value.toLowerCase())) {
                if (PREFIX.evaluateRaw(token,prefix.toLowerCase())) return true;
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
    CONTAINS_REGEX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value,condition);
            if (value == null) return false;
            return evaluateRaw(value.toString(),(String)condition);
        }

        @Override
        public boolean evaluateRaw(String value, String regex) {
            for (String token : tokenize(value.toLowerCase())) {
                if (REGEX.evaluateRaw(token,regex)) return true;
            }
            return false;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition != null && condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

    },

    /**
     * Whether the text starts with a given prefix (case sensitive)
     */
    PREFIX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value,condition);
            if (value==null) return false;
            return evaluateRaw(value.toString(),(String)condition);
        }

        @Override
        public boolean evaluateRaw(String value, String prefix) {
            return value.startsWith(prefix.trim());
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition != null && condition instanceof String;
        }

    },

    /**
     * Whether the text matches a regular expression (case sensitive)
     */
    REGEX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value,condition);
            if (value == null) return false;
            return evaluateRaw(value.toString(),(String)condition);
        }

        public boolean evaluateRaw(String value, String regex) {
            return value.matches(regex);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition != null && condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

    };

    private static final Logger log = LoggerFactory.getLogger(Text.class);

    public void preevaluate(Object value, Object condition) {
        Preconditions.checkArgument(this.isValidCondition(condition), "Invalid condition provided: %s", condition);
        if (!(value instanceof String)) log.debug("Value not a string: " + value);
    }

    abstract boolean evaluateRaw(String value, String condition);

    private static final int MIN_TOKEN_LENGTH = 1;

    public static List<String> tokenize(String str) {
        ArrayList<String> tokens = new ArrayList<String>();
        int previous = 0;
        for (int p = 0; p < str.length(); p++) {
            if (!Character.isLetterOrDigit(str.charAt(p))) {
                if (p > previous + MIN_TOKEN_LENGTH) tokens.add(str.substring(previous, p));
                previous = p + 1;
            }
        }
        if (previous + MIN_TOKEN_LENGTH < str.length()) tokens.add(str.substring(previous, str.length()));
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

    //////////////// statics

    public static <V> P<V> textContains(final V value) {
        return new P(Text.CONTAINS, value);
    }
    public static <V> P<V> textContainsPrefix(final V value) {
        return new P(Text.CONTAINS_PREFIX, value);
    }
    public static <V> P<V> textContainsRegex(final V value) {
        return new P(Text.CONTAINS_REGEX, value);
    }
    public static <V> P<V> textPrefix(final V value) {
        return new P(Text.PREFIX, value);
    }
    public static <V> P<V> textRegex(final V value) {
        return new P(Text.REGEX, value);
    }
}
