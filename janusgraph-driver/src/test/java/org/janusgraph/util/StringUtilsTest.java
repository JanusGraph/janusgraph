// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.util;

import org.janusgraph.core.attribute.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Locale;

import static org.janusgraph.util.StringUtils.lowerCaseCodePoints;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Text lower-cased one code point at a time, as the analyzers of the index backends lower-case it, whatever the
 * default locale.
 */
public class StringUtilsTest {

    @Test
    public void lowerCasesOneCodePointAtATime() {
        // The dotted capital I is an i, not an i and a combining dot above as String.toLowerCase makes it
        assertEquals("istanbul", lowerCaseCodePoints("İSTANBUL"));
        // A capital sigma is a sigma at the end of a word too, not a final sigma
        assertEquals("οδοσ", lowerCaseCodePoints("ΟΔΟΣ"));
        // A letter outside the Basic Multilingual Plane, the Deseret capital long I
        assertEquals("a𐐨b", lowerCaseCodePoints("A𐐀B"));
        assertEquals("mixed case", lowerCaseCodePoints("MiXeD CaSe"));
        final String lowerCase = "nothing to lower-case 123";
        assertSame(lowerCase, lowerCaseCodePoints(lowerCase));
    }

    @Test
    @ResourceLock(Resources.LOCALE)
    public void lowerCasesTheCapitalIToAnIInATurkishDefaultLocale() {
        final Locale defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        try {
            assertEquals("isparta", lowerCaseCodePoints("ISPARTA"));
            assertEquals("istanbul", lowerCaseCodePoints("İSTANBUL"));
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }

    // Text predicates evaluated in memory lower-case with it, and so find the dotted capital I as the index backends do
    @Test
    public void textPredicatesFindTheDottedCapitalI() {
        assertTrue(Text.CONTAINS.test("İSTANBUL", "istanbul"));
        assertTrue(Text.CONTAINS_PREFIX.test("İZMİR", "izm"));
        assertTrue(Text.CONTAINS_PHRASE.test("VISIT İZMİR", "visit izmir"));
    }
}
