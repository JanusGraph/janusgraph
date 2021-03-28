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

package org.janusgraph.diskstorage.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ArrayUtilTest {

    @Test
    void testGrowSpace() {
        // small capacity
        assertEquals(6, ArrayUtil.growSpace(3, 5));
        assertEquals(10, ArrayUtil.growSpace(3, 10));
        assertEquals(10, ArrayUtil.growSpace(10, 8));
        assertEquals(10, ArrayUtil.growSpace(10, 10));

        // minCapacity is negative (usually caused by overflow then calculating minCapacity)
        assertThrows(IllegalArgumentException.class, () -> ArrayUtil.growSpace(100, Integer.MAX_VALUE + 1));
        assertThrows(IllegalArgumentException.class, () -> ArrayUtil.growSpace(100, -1));
        assertThrows(IllegalArgumentException.class, () -> ArrayUtil.growSpace(Integer.MAX_VALUE - 100, Integer.MAX_VALUE + 100));

        // minCapacity larger than MAX_ARRAY_SIZE
        assertThrows(IllegalArgumentException.class, () -> ArrayUtil.growSpace(100, Integer.MAX_VALUE - 7));
        assertThrows(IllegalArgumentException.class, () -> ArrayUtil.growSpace(100, Integer.MAX_VALUE));

        // allocate MAX_ARRAY_SIZE when old capacity is larger than MAX_ARRAY_SIZE / 2
        assertEquals(Integer.MAX_VALUE - 8, ArrayUtil.growSpace(Integer.MAX_VALUE - 100, Integer.MAX_VALUE - 50));
        assertEquals(Integer.MAX_VALUE - 8, ArrayUtil.growSpace(Integer.MAX_VALUE / 2, Integer.MAX_VALUE / 2 + 100));
        assertEquals(Integer.MAX_VALUE - 8, ArrayUtil.growSpace(Integer.MAX_VALUE / 2 - 3, Integer.MAX_VALUE - 8));
        assertEquals(Integer.MAX_VALUE - 8, ArrayUtil.growSpace(Integer.MAX_VALUE / 2 - 3, Integer.MAX_VALUE / 2 + 100));
        assertEquals(Integer.MAX_VALUE - 8, ArrayUtil.growSpace((int) (Integer.MAX_VALUE * 0.7), (int) (Integer.MAX_VALUE * 0.7) + 1));
        assertEquals((Integer.MAX_VALUE / 2 - 4) * 2, ArrayUtil.growSpace(Integer.MAX_VALUE / 2 - 4, Integer.MAX_VALUE / 2 + 100));

        // old capacity already reaches MAX_ARRAY_SIZE
        assertEquals(Integer.MAX_VALUE - 8, ArrayUtil.growSpace(Integer.MAX_VALUE - 8, Integer.MAX_VALUE - 100));
        assertEquals(Integer.MAX_VALUE - 8, ArrayUtil.growSpace(Integer.MAX_VALUE - 8, Integer.MAX_VALUE - 8));
    }
}
