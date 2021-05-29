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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.NOT_CONTAINS;
        }

        @Override
        public String toString() {
            return "textContains";
        }

    },

    /**
     * Whether the text doesnt contain a given term as a token in the text (case insensitive)
     */
    NOT_CONTAINS {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String terms) {
            return !CONTAINS.evaluateRaw(value, terms);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return CONTAINS.isValidCondition(condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.CONTAINS;
        }

        @Override
        public String toString() {
            return "textNotContains";
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
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.NOT_CONTAINS_PREFIX;
        }

        @Override
        public String toString() {
            return "textContainsPrefix";
        }

    },

    /**
     * Whether the text doesnt contain a token that starts with a given term (case insensitive)
     */
    NOT_CONTAINS_PREFIX {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String prefix) {
            return !CONTAINS_PREFIX.evaluateRaw(value, prefix);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return CONTAINS_PREFIX.isValidCondition(condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.CONTAINS_PREFIX;
        }

        @Override
        public String toString() {
            return "textNotContainsPrefix";
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
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.NOT_CONTAINS_REGEX;
        }

        @Override
        public String toString() {
            return "textContainsRegex";
        }

    },

    /**
     * Whether the text doesnt contain a token that matches a regular expression
     */
    NOT_CONTAINS_REGEX {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String regex) {
            return !CONTAINS_REGEX.evaluateRaw(value, regex);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return CONTAINS_REGEX.isValidCondition(condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.CONTAINS_REGEX;
        }

        @Override
        public String toString() {
            return "textNotContainsRegex";
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
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.NOT_PREFIX;
        }

        @Override
        public String toString() {
            return "textPrefix";
        }

    },

    /**
     * Whether the text doesnt start with a given prefix (case sensitive)
     */
    NOT_PREFIX {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String prefix) {
            return !value.startsWith(prefix.trim());
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return PREFIX.isValidCondition(condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.PREFIX;
        }

        @Override
        public String toString() {
            return "textNotPrefix";
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
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.NOT_REGEX;
        }

        @Override
        public String toString() {
            return "textRegex";
        }

    },

    /**
     * Whether the text fails a regular expression (case sensitive)
     */
    NOT_REGEX {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        public boolean evaluateRaw(String value, String regex) {
            return !value.matches(regex);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return REGEX.isValidCondition(condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.REGEX;
        }

        @Override
        public String toString() {
            return "textNotRegex";
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
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.NOT_FUZZY;
        }

        @Override
        public String toString() {
            return "textFuzzy";
        }

    },

    /**
     * Whether the text is not at X Levenshtein of a token (case sensitive)
     * with X=:
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     */
    NOT_FUZZY {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String term) { return !isFuzzy(term.trim(),value.trim()); }

        @Override
        public boolean isValidCondition(Object condition) {
            return FUZZY.isValidCondition(condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.FUZZY;
        }

        @Override
        public String toString() {
            return "textNotFuzzy";
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
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.NOT_CONTAINS_FUZZY;
        }

        @Override
        public String toString() {
            return "textContainsFuzzy";
        }

    },

    /**
     * Whether the text doesnt contain a token is at X Levenshtein of a token (case insensitive)
     * with X=:
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     */
    NOT_CONTAINS_FUZZY {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String term) {
            return !CONTAINS_FUZZY.evaluateRaw(value, term);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return CONTAINS_FUZZY.isValidCondition(condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.CONTAINS_FUZZY;
        }

        @Override
        public String toString() {
            return "textNotContainsFuzzy";
        }

    },

    /**
     * Whether the text contains a given token sequence in the text (case insensitive)
     */
    CONTAINS_PHRASE {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String terms) {
            List<String> valueTerms = tokenize(value.trim().toLowerCase());
            List<String> tokenTerms = tokenize(terms.trim().toLowerCase());
            if (!terms.isEmpty() && tokenTerms.isEmpty()) return false;
            return (Collections.indexOfSubList(valueTerms, tokenTerms) != -1);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank((String) condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.NOT_CONTAINS_PHRASE;
        }

        @Override
        public String toString() {
            return "textContainsPhrase";
        }

    },

    /**
     * Whether the text does not contains a given token sequence in the text (case insensitive)
     */
    NOT_CONTAINS_PHRASE {

        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String terms) {
            return !CONTAINS_PHRASE.evaluateRaw(value, terms);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return CONTAINS_PHRASE.isValidCondition(condition);
        }

        @Override
        public boolean hasNegation() {
            return true;
        }

        @Override
        public JanusGraphPredicate negate() {
            return Text.CONTAINS_PHRASE;
        }

        @Override
        public String toString() {
            return "textNotContainsPhrase";
        }

    };

    /**
     * Calculates the max fuzzy edit distance for a term given its length
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     * @param term
     * @return
     */
    public static int getMaxEditDistance(String term) {
        if (term.length() < 3)
            return 0;
        else if (term.length() < 6)
            return 1;
        else
            return 2;
    }

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

    public static final Set<Text> HAS_CONTAINS = Collections
            .unmodifiableSet(EnumSet.of(CONTAINS, CONTAINS_PREFIX, CONTAINS_REGEX, CONTAINS_FUZZY, CONTAINS_PHRASE,
                NOT_CONTAINS, NOT_CONTAINS_PREFIX, NOT_CONTAINS_REGEX, NOT_CONTAINS_FUZZY, NOT_CONTAINS_PHRASE));

    public static <V> JanusGraphP textContains(final V value) {
        return new JanusGraphP(Text.CONTAINS, value);
    }
    public static <V> JanusGraphP textNotContains(final V value) {
        return new JanusGraphP(Text.NOT_CONTAINS, value);
    }
    public static <V> JanusGraphP textContainsPrefix(final V value) {
        return new JanusGraphP(Text.CONTAINS_PREFIX, value);
    }
    public static <V> JanusGraphP textNotContainsPrefix(final V value) {
        return new JanusGraphP(Text.NOT_CONTAINS_PREFIX, value);
    }
    public static <V> JanusGraphP textContainsRegex(final V value) {
        return new JanusGraphP(Text.CONTAINS_REGEX, value);
    }
    public static <V> JanusGraphP textNotContainsRegex(final V value) {
        return new JanusGraphP(Text.NOT_CONTAINS_REGEX, value);
    }
    public static <V> JanusGraphP textPrefix(final V value) {
        return new JanusGraphP(Text.PREFIX, value);
    }
    public static <V> JanusGraphP textNotPrefix(final V value) {
        return new JanusGraphP(Text.NOT_PREFIX, value);
    }
    public static <V> JanusGraphP textRegex(final V value) {
        return new JanusGraphP(Text.REGEX, value);
    }
    public static <V> JanusGraphP textNotRegex(final V value) {
        return new JanusGraphP(Text.NOT_REGEX, value);
    }
    public static <V> JanusGraphP textContainsFuzzy(final V value) {
        return new JanusGraphP(Text.CONTAINS_FUZZY, value);
    }
    public static <V> JanusGraphP textNotContainsFuzzy(final V value) {
        return new JanusGraphP(Text.NOT_CONTAINS_FUZZY, value);
    }
    public static <V> JanusGraphP textFuzzy(final V value) {
        return new JanusGraphP(Text.FUZZY, value);
    }
    public static <V> JanusGraphP textNotFuzzy(final V value) {
        return new JanusGraphP(Text.NOT_FUZZY, value);
    }
    public static <V> JanusGraphP textContainsPhrase(final V value) {
        return new JanusGraphP(Text.CONTAINS_PHRASE, value);
    }
    public static <V> JanusGraphP textNotContainsPhrase(final V value) {
        return new JanusGraphP(Text.NOT_CONTAINS_PHRASE, value);
    }
}
