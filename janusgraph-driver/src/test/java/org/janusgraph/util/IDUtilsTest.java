// Copyright 2022 JanusGraph Authors
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

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IDUtilsTest {
    @Test
    public void testCheckId() {
        IDUtils.checkId("a-string-id");
        IDUtils.checkId(1);
        assertThrows(IllegalArgumentException.class, () -> IDUtils.checkId(null));
        assertThrows(IllegalArgumentException.class, () -> IDUtils.checkId(-1));
        assertThrows(IllegalArgumentException.class, () -> IDUtils.checkId(0));
        assertThrows(IllegalArgumentException.class, () -> IDUtils.checkId(UUID.randomUUID()));
    }

    @Test
    public void testCompare() {
        assertTrue(IDUtils.compare(123, "x") < 0);
        assertTrue(IDUtils.compare("x", 123) > 0);
        assertTrue(IDUtils.compare("x", "y") < 0);
        assertTrue(IDUtils.compare(123, 123L) == 0);
        assertTrue(IDUtils.compare(Long.MAX_VALUE - 1, Long.MAX_VALUE) < 0);
        assertTrue(IDUtils.compare(Long.MAX_VALUE, Long.MAX_VALUE) == 0);
        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        Exception ex = assertThrows(IllegalArgumentException.class, () -> IDUtils.compare(uuid, uuid2));
        assertEquals("Cannot compare ids: " + uuid + ", " + uuid2, ex.getMessage());
    }
}
