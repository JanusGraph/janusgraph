// Copyright 2021 JanusGraph Authors
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

import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Predicate test parameters
 *
 * @author Andrew Sheppard (andrew.sheppard@fireeye.com)
 */
public class TextArgument {

    public static final String text = "This world is full of 1funny surprises! A Full Yes";
    public static final String name = "fully funny";
    public static final String shortValue = "ah";
    public static final String mediumValue = "hop";
    public static final String longValue = "surprises";

    private static Stream<Arguments> addPredicate(JanusGraphPredicate predicate, Stream<Arguments> argStream) {
        return argStream.map(argList -> {
            Object[] rawArgs = argList.get();
            return arguments(predicate, rawArgs[0], rawArgs[1], rawArgs[2]);
        });
    }

    private static Stream<Arguments> negate(Stream<Arguments> argStream) {
        return argStream.map(argList -> {
            Object[] rawArgs = argList.get();
            if (rawArgs[1] == null)
                // null values have the same result in both negated and non negated tests
                return argList;
            else
                return arguments(!((boolean) rawArgs[0]), rawArgs[1], rawArgs[2]);
        });
    }

    /**
     * Common arguments for Text.CONTAINS and Text.NOT_CONTAINS tests
     *
     * @return
     */
    private static Stream<Arguments> textContainsCommon() {
        return Arrays.stream(new Arguments[] {
            arguments(true, text, "world"),
            arguments(true, text, "wOrLD"),
            arguments(false, text, "worl"),
    
            arguments(true, text, "this"),
            arguments(true, text, "yes"),
            arguments(false, text, "funny"),
    
            arguments(true, text, "surprises"),
            arguments(true, text, "FULL"),

            arguments(true, text, "full surprises"),
            arguments(true, text, "full,surprises,world"),
            arguments(true, text, "a world"),

            arguments(false, text, "full bunny"),

            // null value
            arguments(false, null, "anything")
        });
    }

    /**
     * Common arguments for Text.PREFIX and Text.NOT_PREFIX tests
     *
     * @return
     */
    private static Stream<Arguments> textPrefixCommon() {
        return Arrays.stream(new Arguments[] {
            arguments(true, name, "fully"),
            arguments(true, name, "ful"),
            arguments(true, name, "fully fu"),
            arguments(false, name, "fun"),

            // null value
            arguments(false, null, "anything")
        });
    }

    /**
     * Common arguments for Text.CONTAINS_PREFIX and Text.NOT_CONTAINS_PREFIX tests
     *
     * @return
     */
    private static Stream<Arguments> textContainsPrefixCommon() {
        return Arrays.stream(new Arguments[] {
            arguments(true, name, "fully"),
            arguments(true, name, "ful"),

            arguments(true, text, "worl"),
            arguments(true, text, "wORl"),
            arguments(true, text, "ye"),
            arguments(true, text, "Y"),

            arguments(false, text, "fo"),
            arguments(false, text, "of 1f"),
            arguments(false, text, "ses"),

            arguments(true, name, "fun"),

            // null value
            arguments(false, null, "anything")
        });
    }

    /**
     * Common arguments for Text.REGEX and Text.NOT_REGEX tests
     *
     * @return
     */
    private static Stream<Arguments> textRegexCommon() {
        return Arrays.stream(new Arguments[] {
            // tailing wildcard
            arguments(true, "over", "o.*"),
            arguments(true, "over", "ove.?"),
            arguments(true, "over", "ove[rst]?"),

            // leading wildcard
            arguments(true, "over", ".*r"),
            arguments(true, "over", ".*ver"),
            arguments(true, "over", ".?ver"),
            arguments(true, "over", "[opr]?ver"),

            // inner wildcard
            arguments(true, "over", "o.*r"),
            arguments(true, "over", "o.*er"),
            arguments(true, "over", "o.?er"),
            arguments(true, "over", "o[ve]*r"),
            arguments(true, "over", "o.+r"),

            arguments(true, name, "(fu[ln]*y) (fu[ln]*y)"),
            arguments(false, name, "(fu[l]*y) (fu[l]*y)"),
            arguments(true, name, "(fu[l]*y) .*"),

            // null value
            arguments(false, null, "anything")
        });
    }

    /**
     * Common arguments for Text.CONTAINS_REGEX and Text.NOT_CONTAINS_REGEX tests
     *
     * @return
     */
    private static Stream<Arguments> textContainsRegexCommon() {
        return Arrays.stream(new Arguments[] {
            // tailing wildcard
            arguments(true, "over", "o.*"),
            arguments(true, "over", "ove.?"),
            arguments(true, "over", "ove[rst]?"),

            // leading wildcard
            arguments(true, "over", ".*r"),
            arguments(true, "over", ".*ver"),
            arguments(true, "over", ".?ver"),
            arguments(true, "over", "[opr]?ver"),

            // inner wildcard
            arguments(true, "over", "o.*r"),
            arguments(true, "over", "o.*er"),
            arguments(true, "over", "o.?er"),
            arguments(true, "over", "o[ve]*r"),
            arguments(true, "over", "o.+r"),

            arguments(true, text, "fu[l]+"),
            arguments(true, text, "wor[ld]{1,2}"),

            arguments(false, text, "fo"),
            arguments(false, text, "wor[l]+"),
            arguments(false, text, "wor[ld]{3,5}"),

            // null value
            arguments(false, null, "anything")
        });
    }

    /**
     * Common arguments for Text.CONTAINS_PHRASE and Text.NOT_CONTAINS_PHRASE tests
     *
     * @return
     */
    private static Stream<Arguments> textContainsPhraseCommon() {
        return Arrays.stream(new Arguments[] {
            arguments(true, text, "world"),
            arguments(true, text, "wOrLD"),
            arguments(false, text, "worl"),

            arguments(true, text, "this"),
            arguments(true, text, "yes"),
            arguments(false, text, "funny"),

            arguments(true, text, "surprises"),
            arguments(true, text, "FULL"),

            arguments(false, text, "full surprises"),
            arguments(false, text, "full,surprises,world"),

            arguments(true, text, "is full of 1funny"),
            arguments(true, text, "This world is"),
            arguments(false, text, "A Full Yes Or No"),

            // null value
            arguments(false, null, "anything")
        });
    }

    /**
     * Common arguments for Text.FUZZY and Text.NOT_FUZZY tests
     *
     * @return
     */
    private static Stream<Arguments> textFuzzyCommon() {
        return Arrays.stream(new Arguments[] {
            // Short
            arguments(true, shortValue, "ah"),
            arguments(false, shortValue, "ai"),

            // Medium
            arguments(true, mediumValue, "hop"),
            arguments(true, mediumValue, "hopp"),
            arguments(true, mediumValue, "hap"),
            arguments(false, mediumValue, "ha"),
            arguments(false, mediumValue, "hoopp"),

            // Long
            arguments(true, longValue, "surprises"),
            arguments(true, longValue, "surpprises"),
            arguments(true, longValue, "sutprises"),
            arguments(true, longValue, "surprise"),
            arguments(false, longValue, "surppirsses"),

            // null value
            arguments(false, null, "anything")
        });
    }

    /**
     * Common arguments for Text.CONTAINS_FUZZY and Text.NOT_CONTAINS_FUZZY tests
     *
     * @return
     */
    private static Stream<Arguments> textContainsFuzzyCommon() {
        return Arrays.stream(new Arguments[] {
            // Short
            arguments(true, shortValue, "ah"),
            arguments(false, shortValue, "ai"),

            // Medium
            arguments(true, mediumValue, "hop"),
            arguments(true, mediumValue, "hopp"),
            arguments(true, mediumValue, "hap"),
            arguments(false, mediumValue, "ha"),
            arguments(false, mediumValue, "hoopp"),

            // Long
            arguments(true, longValue, "surprises"),
            arguments(true, longValue, "surpprises"),
            arguments(true, longValue, "sutprises"),
            arguments(true, longValue, "surprise"),
            arguments(false, longValue, "surppirsses"),

            // Short
            arguments(true, text, "is"),
            arguments(false, text, "si"),

            // Medium
            arguments(true, text, "full"),
            arguments(true, text, "fully"),
            arguments(true, text, "ful"),
            arguments(true, text, "fill"),
            arguments(false, text, "fu"),
            arguments(false, text, "fullest"),

            // Long
            arguments(true, text, "surprises"),
            arguments(true, text, "Surpprises"),
            arguments(true, text, "Sutrises"),
            arguments(true, text, "surprise"),
            arguments(false, text, "surppirsses"),

            // null value
            arguments(false, null, "anything")
        });
    }

    /**
     * Generates arguments for Text.CONTAINS tests
     *
     * @return
     */
    public static Stream<Arguments> textContains() {
        return addPredicate(Text.CONTAINS, textContainsCommon());
    }

    /**
     * Generates arguments for Text.NOT_CONTAINS tests
     *
     * @return
     */
    public static Stream<Arguments> textNotContains() {
        return addPredicate(Text.NOT_CONTAINS, negate(textContainsCommon()));
    }

    /**
     * Generates arguments for Text.PREFIX tests
     *
     * @return
     */
    public static Stream<Arguments> textPrefix() {
        return addPredicate(Text.PREFIX, textPrefixCommon());
    }

    /**
     * Generates arguments for Text.NOT_PREFIX tests
     *
     * @return
     */
    public static Stream<Arguments> textNotPrefix() {
        return addPredicate(Text.NOT_PREFIX, negate(textPrefixCommon()));
    }

    /**
     * Generates arguments for Text.CONTAINS_PREFIX tests
     *
     * @return
     */
    public static Stream<Arguments> textContainsPrefix() {
        return addPredicate(Text.CONTAINS_PREFIX, textContainsPrefixCommon());
    }

    /**
     * Generates arguments for Text.NOT_CONTAINS_PREFIX tests
     *
     * @return
     */
    public static Stream<Arguments> textNotContainsPrefix() {
        return addPredicate(Text.NOT_CONTAINS_PREFIX, negate(textContainsPrefixCommon()));
    }

    /**
     * Generates arguments for Text.REGEX tests
     *
     * @return
     */
    public static Stream<Arguments> textRegex() {
        return addPredicate(Text.REGEX, textRegexCommon());
    }

    /**
     * Generates arguments for Text.NOT_REGEX tests
     *
     * @return
     */
    public static Stream<Arguments> textNotRegex() {
        return addPredicate(Text.NOT_REGEX, negate(textRegexCommon()));
    }

    /**
     * Generates arguments for Text.CONTAINS_REGEX tests
     *
     * @return
     */
    public static Stream<Arguments> textContainsRegex() {
        return addPredicate(Text.CONTAINS_REGEX, textContainsRegexCommon());
    }

    /**
     * Generates arguments for Text.NOT_CONTAINS_REGEX tests
     *
     * @return
     */
    public static Stream<Arguments> textNotContainsRegex() {
        return addPredicate(Text.NOT_CONTAINS_REGEX, negate(textContainsRegexCommon()));
    }

    /**
     * Generates arguments for Text.CONTAINS_PHRASE tests
     *
     * @return
     */
    public static Stream<Arguments> textContainsPhrase() {
        return addPredicate(Text.CONTAINS_PHRASE, textContainsPhraseCommon());
    }

    /**
     * Generates arguments for Text.NOT_CONTAINS_PHRASE tests
     *
     * @return
     */
    public static Stream<Arguments> textNotContainsPhrase() {
        return addPredicate(Text.NOT_CONTAINS_PHRASE, negate(textContainsPhraseCommon()));
    }

    /**
     * Generates arguments for Text.FUZZY tests
     *
     * @return
     */
    public static Stream<Arguments> textFuzzy() {
        return addPredicate(Text.FUZZY, textFuzzyCommon());
    }

    /**
     * Generates arguments for Text.NOT_FUZZY tests
     *
     * @return
     */
    public static Stream<Arguments> textNotFuzzy() {
        return addPredicate(Text.NOT_FUZZY, negate(textFuzzyCommon()));
    }

    /**
     * Generates arguments for Text.CONTAINS_FUZZY tests
     *
     * @return
     */
    public static Stream<Arguments> textContainsFuzzy() {
        return addPredicate(Text.CONTAINS_FUZZY, textContainsFuzzyCommon());
    }

    /**
     * Generates arguments for Text.NOT_CONTAINS_FUZZY tests
     *
     * @return
     */
    public static Stream<Arguments> textNotContainsFuzzy() {
        return addPredicate(Text.NOT_CONTAINS_FUZZY, negate(textContainsFuzzyCommon()));
    }

    /**
     * Returns an argument list for all string predicates
     *
     * @return
     */
    public static Stream<Arguments> string() {
        return Stream.of(
            textFuzzy(),
            textNotFuzzy(),

            textPrefix(),
            textNotPrefix(),

            textRegex(),
            textNotRegex()
        ).flatMap(ii -> ii);
    }

    /**
     * Returns an argument list for all text predicates
     *
     * @return
     */
    public static Stream<Arguments> text() {
        return Stream.of(
            textContains(),
            textNotContains(),

            textContainsFuzzy(),
            textNotContainsFuzzy(),

            textContainsPhrase(),
            textNotContainsPhrase(),

            textContainsPrefix(),
            textNotContainsPrefix(),

            textContainsRegex(),
            textNotContainsRegex()
        ).flatMap(ii -> ii);
    }

    /**
     * Returns an argument list for all predicates
     *
     * @return
     */
    public static Stream<Arguments> all() {
        return Stream.concat(string(), text());
    }
}
