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

package org.janusgraph.graphdb.transaction;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import static org.janusgraph.graphdb.transaction.StandardJanusGraphTx.isRangedAsOrdered;
import static org.janusgraph.graphdb.transaction.StandardJanusGraphTx.rangeBound;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * When a graph query whose new elements fill its limit may narrow the index query by a range on its first order key,
 * and from which bound: only where every index compares the bound with the field's values as memory orders them, but
 * for some stored strings which aren't valid UTF-16.
 */
public class StandardJanusGraphTxOrderRangeTest {

    private final PropertyKey key = mock(PropertyKey.class);

    private ParameterIndexField field(SchemaStatus status, Parameter... parameters) {
        final Parameter[] all = Arrays.copyOf(parameters, parameters.length + 1);
        all[parameters.length] = ParameterType.STATUS.getParameter(status);
        return ParameterIndexField.of(key, all);
    }

    @Test
    public void takesARangeForWholeNumbersAndDatesInBothDirections() {
        final ParameterIndexField field = field(SchemaStatus.ENABLED);
        for (final Object value : Arrays.asList(1L, 1, (short) 1, (byte) 1, new Date(0),
                Instant.ofEpochSecond(1, 5_000_000))) {
            assertTrue(isRangedAsOrdered(value, field, true), value::toString);
            assertTrue(isRangedAsOrdered(value, field, false), value::toString);
        }
    }

    @Test
    public void takesARangeForDecimalsOnlyInAnAscendingOrder() {
        final ParameterIndexField field = field(SchemaStatus.ENABLED);
        assertTrue(isRangedAsOrdered(1.5, field, false));
        assertTrue(isRangedAsOrdered(1.5f, field, false));
        assertFalse(isRangedAsOrdered(1.5, field, true));
        assertFalse(isRangedAsOrdered(1.5f, field, true));
        assertFalse(isRangedAsOrdered(Double.NaN, field, false));
        assertFalse(isRangedAsOrdered(Float.NaN, field, false));
    }

    //A TEXTSTRING key's ranges and order read the field of its whole strings, as a STRING key's read its own field
    @Test
    public void takesARangeForAStringOnlyOnAFieldWhichComparesItWhole() {
        for (final Mapping mapping : Arrays.asList(Mapping.STRING, Mapping.TEXTSTRING)) {
            final ParameterIndexField field = field(SchemaStatus.ENABLED, mapping.asParameter());
            assertTrue(isRangedAsOrdered("m", field, true), mapping::toString);
            assertTrue(isRangedAsOrdered("m", field, false), mapping::toString);
            assertTrue(isRangedAsOrdered("", field, true), mapping::toString);
            assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, mapping.asParameter(),
                ParameterType.STRING_ANALYZER.getParameter("keyword")), true), mapping::toString);
            assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, mapping.asParameter(),
                ParameterType.TEXT_ANALYZER.getParameter("standard")), true), mapping::toString);
        }
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, Mapping.TEXT.asParameter()), true));
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED), true));
    }

    //Characters from U+E000 to U+FFFF and from U+10000 up, as surrogate pairs, which memory orders by code point too
    @Test
    public void takesARangeForAStringOfValidUtf16() {
        for (final Mapping mapping : Arrays.asList(Mapping.STRING, Mapping.TEXTSTRING)) {
            final ParameterIndexField field = field(SchemaStatus.ENABLED, mapping.asParameter());
            for (final String value : Arrays.asList("\uD7FF", "\uE000", "a\uE000", "\uFFFD", "\uFFFF",
                    "a\uD83D\uDE00", "\uD800\uDC00", "\uDBFF\uDFFF", "\uD83D\uDE00\uD83D\uDE01b")) {
                assertTrue(isRangedAsOrdered(value, field, true), () -> mapping + " " + codeUnits(value));
                assertTrue(isRangedAsOrdered(value, field, false), () -> mapping + " " + codeUnits(value));
            }
        }
    }

    //No index holds a surrogate without its pair as it is
    @Test
    public void takesNoRangeForAStringWithASurrogateOutOfItsPair() {
        final ParameterIndexField field = field(SchemaStatus.ENABLED, Mapping.STRING.asParameter());
        for (final String value : Arrays.asList("\uD800", "a\uDBFF", "a\uDC00", "\uD83Da", "\uDE00\uD83D",
                "\uD83D\uD83D\uDE00", "\uD83D\uDE00\uDE00")) {
            assertFalse(isRangedAsOrdered(value, field, true), () -> codeUnits(value));
            assertFalse(isRangedAsOrdered(value, field, false), () -> codeUnits(value));
        }
    }

    @Test
    public void takesNoRangeOnAFieldWhichIsntEnabledOrHasCustomParameters() {
        assertFalse(isRangedAsOrdered(1L, field(SchemaStatus.REGISTERED), true));
        assertFalse(isRangedAsOrdered(1L, field(SchemaStatus.INSTALLED), true));
        assertFalse(isRangedAsOrdered(1L, field(SchemaStatus.DISABLED), true));
        assertFalse(isRangedAsOrdered(1L, field(SchemaStatus.ENABLED,
            Parameter.of(ParameterType.CUSTOM_PARAMETER_PREFIX + "index", false)), true));
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, Mapping.STRING.asParameter(),
            Parameter.of(ParameterType.CUSTOM_PARAMETER_PREFIX + "normalizer", "lowercase")), true));
    }

    @Test
    public void takesNoRangeForOtherTypesOrAnInstantFinerThanMilliseconds() {
        final ParameterIndexField field = field(SchemaStatus.ENABLED);
        for (final Object value : Arrays.asList(true, UUID.randomUUID(), 'm', new BigDecimal("1.5"),
                Geoshape.point(1, 2), Instant.ofEpochSecond(1, 5))) {
            assertFalse(isRangedAsOrdered(value, field, true), value::toString);
            assertFalse(isRangedAsOrdered(value, field, false), value::toString);
        }
    }

    //A descending range of a string with a character from U+FFFD up starts at U+FFFD in place of the first of them
    @Test
    public void cutsTheDescendingBoundOfAStringBeforeItsFirstCharacterFromUFFFDUp() {
        assertEquals("m", rangeBound("m", true));
        assertEquals("", rangeBound("", true));
        assertEquals("\uE000\uFFFC", rangeBound("\uE000\uFFFC", true));
        assertEquals("a\uFFFD", rangeBound("a\uFFFDb", true));
        assertEquals("a\uFFFD", rangeBound("a\uFFFE", true));
        assertEquals("ab\uFFFD", rangeBound("ab\uFFFF\uE000", true));
        assertEquals("a\uFFFD", rangeBound("a\uD83D\uDE00b", true));
        assertEquals("\uFFFD", rangeBound("\uD800\uDC00", true));
        //An ascending order, and a value which isn't a string, take the value itself
        for (final Object value : Arrays.asList("a\uD83D\uDE00", "\uFFFF", 5L, 1.5)) {
            assertEquals(value, rangeBound(value, false));
        }
        assertEquals(5L, rangeBound(5L, true));
    }

    //JanusGraph's Lucene backend, OpenSearch 2 and Elasticsearch 6 to 8 build a range from a lower bound of at most
    //1,000 UTF-8 bytes up to the greatest string, and Solr takes a query in a URL of at most 8 KB: a descending bound
    //is cut to its longest prefix of whole characters within 1,000 bytes, which is a lower bound still
    @Test
    public void cutsTheDescendingBoundOfALongStringToItsFirstThousandUtf8Bytes() {
        assertEquals("w".repeat(1000), rangeBound("w".repeat(1001), true));
        assertEquals("w".repeat(1000), rangeBound("w".repeat(1000), true));
        //Characters of three bytes, below and above the surrogates: 333 of them make 999 bytes
        assertEquals("\u4E2D".repeat(333), rangeBound("\u4E2D".repeat(400), true));
        assertEquals("\uE000".repeat(333), rangeBound("\uE000".repeat(400), true));
        //A character of two bytes which would end at byte 1,001
        assertEquals("a".repeat(999), rangeBound("a".repeat(999) + "\u00E9", true));
        //The U+FFFD in place of an emoji, which would end at byte 1,001, or at byte 1,000
        assertEquals("a".repeat(998), rangeBound("a".repeat(998) + "\uD83D\uDE00b", true));
        assertEquals("a".repeat(997) + "\uFFFD", rangeBound("a".repeat(997) + "\uD83D\uDE00", true));
        assertEquals("w".repeat(1000), rangeBound("w".repeat(100_000) + "\uD83D\uDE00", true));
        assertEquals("\u4E2D".repeat(333), rangeBound("\u4E2D".repeat(100_000) + "\uFFFF", true));
    }

    //An upper bound can't be cut: a string of more than 1,000 UTF-8 bytes takes no ascending range
    @Test
    public void takesNoAscendingRangeFromAStringOfMoreThanAThousandUtf8Bytes() {
        assertEquals("w".repeat(1000), rangeBound("w".repeat(1000), false));
        assertNull(rangeBound("w".repeat(1001), false));
        assertEquals("\u4E2D".repeat(333), rangeBound("\u4E2D".repeat(333), false));
        assertNull(rangeBound("\u4E2D".repeat(334), false));
        assertNull(rangeBound("a".repeat(999) + "\u00E9", false));
        assertEquals("a".repeat(996) + "\uD83D\uDE00", rangeBound("a".repeat(996) + "\uD83D\uDE00", false));
        assertNull(rangeBound("a".repeat(997) + "\uD83D\uDE00", false));
    }

    /*
     * The range from the bound holds every stored string which memory orders before the value or at it, in either
     * direction, also one with a surrogate without its pair, which memory places after U+FFFF and Lucene and Solr hold
     * as U+FFFD: random strings of characters from both sides of the surrogates and of surrogates on their own,
     * compared by OrderList as memory compares them, and by their UTF-8 bytes with U+FFFD for each surrogate without
     * its pair as Lucene and Solr compare them
     */
    @Test
    public void theRangeHoldsEveryStoredStringWhichMemoryOrdersUpToTheValue() throws CharacterCodingException {
        final String[] characters = {"a", "z", "\uD7FF", "\uE000", "\uFFFC", "\uFFFD", "\uFFFE", "\uFFFF",
            "\uD800\uDC00", "\uD83D\uDE00", "\uDBFF\uDFFF", "\uD800", "\uD83D", "\uDBFF", "\uDC00", "\uDE00",
            "\uDFFF"};
        final ParameterIndexField field = field(SchemaStatus.ENABLED, Mapping.STRING.asParameter());
        final Random random = new Random(1);
        int ranged = 0;
        for (int i = 0; i < 100_000; i++) {
            final String value = randomString(characters, random);
            final String stored = randomString(characters, random);
            for (final boolean descending : new boolean[]{false, true}) {
                if (!isRangedAsOrdered(value, field, descending)) {
                    continue;
                }
                ranged++;
                if (OrderList.compareOrderValues(stored, value, descending ? Order.DESC : Order.ASC) <= 0) {
                    final String bound = (String) rangeBound(value, descending);
                    final int index = Arrays.compareUnsigned(indexed(stored), indexed(bound));
                    assertTrue(descending ? index >= 0 : index <= 0, () -> (descending ? "descending " : "ascending ")
                        + codeUnits(stored) + " outside the range from " + codeUnits(bound));
                }
            }
        }
        assertTrue(ranged > 50_000, "ranged " + ranged);
    }

    private static String randomString(String[] characters, Random random) {
        final StringBuilder string = new StringBuilder();
        for (int i = random.nextInt(4); i > 0; i--) {
            string.append(characters[random.nextInt(characters.length)]);
        }
        return string.toString();
    }

    //The bytes in which Lucene and Solr hold a string: its UTF-8 bytes, with U+FFFD for a surrogate without its pair
    private static byte[] indexed(String string) throws CharacterCodingException {
        final ByteBuffer bytes = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPLACE)
            .replaceWith(new byte[]{(byte) 0xEF, (byte) 0xBF, (byte) 0xBD}).encode(CharBuffer.wrap(string));
        final byte[] result = new byte[bytes.remaining()];
        bytes.get(result);
        return result;
    }

    private static String codeUnits(String string) {
        final StringBuilder units = new StringBuilder("[");
        for (int i = 0; i < string.length(); i++) {
            units.append(i == 0 ? "" : " ").append(String.format("%04X", (int) string.charAt(i)));
        }
        return units.append(']').toString();
    }
}
