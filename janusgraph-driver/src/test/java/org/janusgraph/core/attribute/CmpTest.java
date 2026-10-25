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

package org.janusgraph.core.attribute;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CmpTest {

    private static final Object[] VALUES =
        {"Tom", 1, 1L, new byte[] {1}, new int[] {1, 2}, Arrays.asList(1, 2), Collections.emptyList()};
    private static final Object[] CONDITIONS =
        {null, "Jerry", "Tom", 1, 2L, new byte[] {1}, new byte[] {2}, new int[] {1, 2}, Arrays.asList(1, 2)};

    @Test
    public void equalAndNotEqualAreNegativesOfEachOther() {
        for (Object value : VALUES) {
            for (Object condition : CONDITIONS) {
                final boolean equal = Cmp.EQUAL.test(value, condition);
                final boolean notEqual = Cmp.NOT_EQUAL.test(value, condition);
                assertNotEquals(equal, notEqual,
                    () -> "both " + equal + " for value " + value + " and condition " + condition);
            }
        }
    }

    @Test
    public void arrayValueMatchesEqualButNotNotEqual() {
        assertTrue(Cmp.EQUAL.test(new byte[] {1}, new byte[] {1}));
        assertFalse(Cmp.NOT_EQUAL.test(new byte[] {1}, new byte[] {1}));
        assertTrue(Cmp.NOT_EQUAL.test(new byte[] {1}, new byte[] {2}));
        assertFalse(Cmp.EQUAL.test(new byte[] {1}, new byte[] {2}));
    }

    @Test
    public void notEqualIsFalseForAnAbsentValue() {
        assertFalse(Cmp.NOT_EQUAL.test(null, "Tom"));
        assertFalse(Cmp.NOT_EQUAL.test(null, new byte[] {1}));
    }
}
