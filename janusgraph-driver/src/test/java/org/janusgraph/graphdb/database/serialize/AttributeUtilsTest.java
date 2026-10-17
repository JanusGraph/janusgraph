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

package org.janusgraph.graphdb.database.serialize;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UUIDs compare by their bits as unsigned numbers, as their string forms do, where UUID.compareTo compares them as
 * signed numbers; other values by their natural order.
 */
public class AttributeUtilsTest {

    //The first digit of either half of each UUID from 8 up, or below it, in the order of their string forms
    private static final List<UUID> BY_STRING = Arrays.asList(UUID.fromString("00000000-0000-0000-0000-000000000001"),
        UUID.fromString("12345678-0000-0000-7fff-ffffffffffff"),
        UUID.fromString("12345678-0000-0000-8000-000000000000"),
        UUID.fromString("7fffffff-ffff-ffff-ffff-ffffffffffff"),
        UUID.fromString("80000000-0000-0000-0000-000000000000"),
        UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));

    @Test
    public void testUUIDsCompareAsTheirStringForms() {
        for (int i = 0; i < BY_STRING.size(); i++) {
            for (int j = 0; j < BY_STRING.size(); j++) {
                final UUID u1 = BY_STRING.get(i), u2 = BY_STRING.get(j);
                final String pair = u1 + " vs " + u2;
                assertEquals(Integer.signum(i - j), Integer.signum(AttributeUtils.compareUnsigned(u1, u2)), pair);
                assertEquals(Integer.signum(i - j), Integer.signum(AttributeUtils.compareValues(u1, u2)), pair);
                assertEquals(Integer.signum(i - j), Integer.signum(AttributeUtils.compare(u1, u2)), pair);
            }
        }
        //UUID.compareTo puts a UUID which starts with a digit from 8 up, in the first half in which two differ, first
        assertTrue(BY_STRING.get(1).compareTo(BY_STRING.get(2)) > 0);
        assertTrue(BY_STRING.get(3).compareTo(BY_STRING.get(4)) > 0);
        final Random random = new Random(5);
        for (int n = 0; n < 20000; n++) {
            final UUID u1 = new UUID(random.nextLong(), random.nextLong());
            final UUID u2 = random.nextBoolean() ? new UUID(u1.getMostSignificantBits(), random.nextLong())
                : new UUID(random.nextLong(), random.nextLong());
            assertEquals(Integer.signum(u1.toString().compareTo(u2.toString())),
                Integer.signum(AttributeUtils.compareUnsigned(u1, u2)), u1 + " vs " + u2);
        }
    }

    @Test
    public void testOtherValuesCompareByTheirNaturalOrder() {
        assertTrue(AttributeUtils.compareValues(1, 2) < 0);
        assertTrue(AttributeUtils.compareValues("b", "a") > 0);
        assertEquals(0, AttributeUtils.compareValues(3L, 3L));
        assertTrue(AttributeUtils.compare(2, 1L) > 0);
        assertNull(AttributeUtils.compare(BY_STRING.get(0), "a"));
        assertNull(AttributeUtils.compare(null, BY_STRING.get(0)));
    }
}
