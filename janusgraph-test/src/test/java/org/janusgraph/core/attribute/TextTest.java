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

import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Andrew Sheppard (andrew.sheppard@fireeye.com)
 */
public class TextTest {

    @ParameterizedTest
    @MethodSource("org.janusgraph.core.attribute.TextArgument#all")
    public void testTextPredicate(JanusGraphPredicate predicate, boolean expected, String value, String condition) {
        assertEquals(expected, predicate.test(value, condition));
    }

    @Test
    public void testNegate() {
        assertTrue(Text.CONTAINS.hasNegation());
        assertEquals(Text.CONTAINS, Text.NOT_CONTAINS.negate());

        assertTrue(Text.NOT_CONTAINS.hasNegation());
        assertEquals(Text.NOT_CONTAINS, Text.CONTAINS.negate());

        assertTrue(Text.CONTAINS_PREFIX.hasNegation());
        assertEquals(Text.CONTAINS_PREFIX, Text.NOT_CONTAINS_PREFIX.negate());

        assertTrue(Text.NOT_CONTAINS_PREFIX.hasNegation());
        assertEquals(Text.NOT_CONTAINS_PREFIX, Text.CONTAINS_PREFIX.negate());

        assertTrue(Text.CONTAINS_REGEX.hasNegation());
        assertEquals(Text.CONTAINS_REGEX, Text.NOT_CONTAINS_REGEX.negate());

        assertTrue(Text.NOT_CONTAINS_REGEX.hasNegation());
        assertEquals(Text.NOT_CONTAINS_REGEX, Text.CONTAINS_REGEX.negate());

        assertTrue(Text.CONTAINS_FUZZY.hasNegation());
        assertEquals(Text.CONTAINS_FUZZY, Text.NOT_CONTAINS_FUZZY.negate());

        assertTrue(Text.NOT_CONTAINS_FUZZY.hasNegation());
        assertEquals(Text.NOT_CONTAINS_FUZZY, Text.CONTAINS_FUZZY.negate());

        assertTrue(Text.CONTAINS_PHRASE.hasNegation());
        assertEquals(Text.CONTAINS_PHRASE, Text.NOT_CONTAINS_PHRASE.negate());

        assertTrue(Text.NOT_CONTAINS_PHRASE.hasNegation());
        assertEquals(Text.NOT_CONTAINS_PHRASE, Text.CONTAINS_PHRASE.negate());

        assertTrue(Text.PREFIX.hasNegation());
        assertEquals(Text.PREFIX, Text.NOT_PREFIX.negate());

        assertTrue(Text.NOT_PREFIX.hasNegation());
        assertEquals(Text.NOT_PREFIX, Text.PREFIX.negate());

        assertTrue(Text.REGEX.hasNegation());
        assertEquals(Text.REGEX, Text.NOT_REGEX.negate());

        assertTrue(Text.NOT_REGEX.hasNegation());
        assertEquals(Text.NOT_REGEX, Text.REGEX.negate());

        assertTrue(Text.FUZZY.hasNegation());
        assertEquals(Text.FUZZY, Text.NOT_FUZZY.negate());

        assertTrue(Text.NOT_FUZZY.hasNegation());
        assertEquals(Text.NOT_FUZZY, Text.FUZZY.negate());
    }

    /**
     * Test for tokenization MIN_TOKEN_LENGTH
     */
    @Test
    public void testTextContainsSmallTokens() {
        assertFalse(Text.CONTAINS.test(TextArgument.text, "a"));
        assertFalse(Text.CONTAINS.test(TextArgument.text, "A"));

        assertTrue(Text.NOT_CONTAINS.test(TextArgument.text, "a"));
        assertTrue(Text.NOT_CONTAINS.test(TextArgument.text, "A"));

        assertFalse(Text.CONTAINS_PHRASE.test(TextArgument.text, "a"));
        assertFalse(Text.CONTAINS_PHRASE.test(TextArgument.text, "A"));

        assertTrue(Text.NOT_CONTAINS_PHRASE.test(TextArgument.text, "a"));
        assertTrue(Text.NOT_CONTAINS_PHRASE.test(TextArgument.text, "A"));
    }

    /**
     * Test for support of regex character groups
     */
    @Test
    public void testTextRegexCharacterGroups() {
        assertTrue(Text.CONTAINS_REGEX.test(TextArgument.text, "\\dfu\\w*"));
        assertTrue(Text.REGEX.test("1funny", "\\dfu\\w*"));

        assertFalse(Text.NOT_CONTAINS_REGEX.test(TextArgument.text, "\\dfu\\w*"));
        assertFalse(Text.NOT_REGEX.test("1funny", "\\dfu\\w*"));
    }

    @Test
    public void testCmp() {
        assertTrue(Cmp.EQUAL.test(TextArgument.name, TextArgument.name));
        assertFalse(Cmp.EQUAL.test("fullly funny", TextArgument.name));

        assertFalse(Cmp.NOT_EQUAL.test(TextArgument.name, TextArgument.name));
        assertTrue(Cmp.NOT_EQUAL.test("fullly funny", TextArgument.name));
    }
}
