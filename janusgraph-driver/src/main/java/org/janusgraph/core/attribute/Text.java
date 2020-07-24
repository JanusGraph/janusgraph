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

import java.util.*;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comparison relations for text objects. These comparisons are based on a tokenized representation
 * of the text, i.e. the text is considered as a set of word tokens.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Text implements JanusGraphPredicate {

    /**
     * Whether the text contains a given term as a token in the text (case insensitive)
     */
    CONTAINS {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String terms) {
            Set<String> tokens = new HashSet<>(tokenize(value.toLowerCase()));
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
            return condition instanceof String && StringUtils.isNotBlank((String) condition);
        }
        @Override
        public String toString() {
            return "textContains";
        }
    },

    /**
     * Whether the text contains a token that starts with a given term (case insensitive)
     */
    CONTAINS_PREFIX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
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
            return condition instanceof String;
        }

        @Override
        public String toString() {
            return "textContainsPrefix";
        }

    },

    /**
     * Whether the text contains a token that matches a regular expression
     */
    CONTAINS_REGEX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
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
            return condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

        @Override
        public String toString() {
            return "textContainsRegex";
        }

    },

    /**
     * Whether the text starts with a given prefix (case sensitive)
     */
    PREFIX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String prefix) {
            return value.startsWith(prefix.trim());
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String;
        }

        @Override
        public String toString() {
            return "textPrefix";
        }

    },

    /**
     * Whether the text matches a regular expression (case sensitive)
     */
    REGEX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        public boolean evaluateRaw(String value, String regex) {
            return value.matches(regex);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

        @Override
        public String toString() {
            return "textRegex";
        }

    }, 
    
    /**
     * Whether the text is at X Levenshtein of a token (case sensitive)
     * with X=:
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     */
    FUZZY {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String term) {
            return isFuzzy(term.trim(),value.trim());
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }
        @Override
        public String toString() {
            return "textFuzzy";
        }

    }, 
    
    /**
     * Whether the text contains a token is at X Levenshtein of a token (case insensitive)
     * with X=:
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     */
    CONTAINS_FUZZY {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String term) {
            for (String token : tokenize(value.toLowerCase())) {
                if (isFuzzy(term.toLowerCase(), token)) return true;
            }
            return false;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }
        @Override
        public String toString() {
            return "textContainsFuzzy";
        }

    };

    private static final LevenshteinDistance ONE_LEVENSHTEIN_DISTANCE = new LevenshteinDistance(1);
    private static final LevenshteinDistance TWO_LEVENSHTEIN_DISTANCE = new LevenshteinDistance(2);

    /**
     * Whether {@code term} is at X Levenshtein of a {@code value} 
     * with X=:
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     * @param value
     * @param term
     * @return true if {@code term} is similar to {@code value} 
     */
    private static boolean isFuzzy(String term, String value){
        term = term.trim();
        if (term.length() < 3) {
            return term.equals(value);
        } else if (term.length() < 6) {
            int levenshteinDistance = ONE_LEVENSHTEIN_DISTANCE.apply(value, term);
            return levenshteinDistance <= 1 && levenshteinDistance >= 0;
        }
        int levenshteinDist = TWO_LEVENSHTEIN_DISTANCE.apply(value, term);
        return levenshteinDist <= 2 && levenshteinDist >= 0;
    }

    private static final Logger log = LoggerFactory.getLogger(Text.class);

    public void preevaluate(Object value, Object condition) {
        Preconditions.checkArgument(this.isValidCondition(condition), "Invalid condition provided: %s", condition);
        if (!(value instanceof String)) log.debug("Value not a string: " + value);
    }

    abstract boolean evaluateRaw(String value, String condition);

    private static final int MIN_TOKEN_LENGTH = 1;

    public static List<String> tokenize(String str) {
        final ArrayList<String> tokens = new ArrayList<>();
        int previous = 0;
        for (int p = 0; p < str.length(); p++) {
            if (!Character.isLetterOrDigit(str.charAt(p))) {
                if (p > previous + MIN_TOKEN_LENGTH) tokens.add(str.substring(previous, p));
                previous = p + 1;
            }
        }
        if (previous + MIN_TOKEN_LENGTH < str.length()) tokens.add(str.substring(previous));
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
    public JanusGraphPredicate negate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isQNF() {
        return true;
    }

    //////////////// statics

    public final static Set<Text> HAS_CONTAINS = Collections
            .unmodifiableSet(EnumSet.of(CONTAINS, CONTAINS_PREFIX, CONTAINS_REGEX, CONTAINS_FUZZY));

    public static <V> JanusGraphP textContains(final V value) {
        return new JanusGraphP(Text.CONTAINS, value);
    }
    public static <V> JanusGraphP textContainsPrefix(final V value) {
        return new JanusGraphP(Text.CONTAINS_PREFIX, value);
    }
    public static <V> JanusGraphP textContainsRegex(final V value) {
        return new JanusGraphP(Text.CONTAINS_REGEX, value);
    }
    public static <V> JanusGraphP textPrefix(final V value) {
        return new JanusGraphP(Text.PREFIX, value);
    }
    public static <V> JanusGraphP textRegex(final V value) {
        return new JanusGraphP(Text.REGEX, value);
    }
    public static <V> JanusGraphP textContainsFuzzy(final V value) {
        return new JanusGraphP(Text.CONTAINS_FUZZY, value);
    }
    public static <V> JanusGraphP textFuzzy(final V value) {
        return new JanusGraphP(Text.FUZZY, value);
    }
}
