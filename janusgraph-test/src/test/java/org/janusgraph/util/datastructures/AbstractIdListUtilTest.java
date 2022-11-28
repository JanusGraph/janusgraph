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

package org.janusgraph.util.datastructures;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractIdListUtilTest {
    @Test
    public void testIsSortedNoUnique() {
        assertTrue(AbstractIdListUtil.isSorted(Arrays.asList("a1", "a2", "b1")));
        assertTrue(AbstractIdListUtil.isSorted(Arrays.asList(2.3, 3.0, 4L, 5, "a1", "a2", "b1")));
        assertTrue(AbstractIdListUtil.isSorted(Arrays.asList(1, 1.0)));
        assertFalse(AbstractIdListUtil.isSorted(Arrays.asList(2, 1.0)));
    }

    @Test
    public void testIsSortedUnique() {
        assertTrue(AbstractIdListUtil.isSorted(Arrays.asList("a1", "a2", "b1"), true));
        assertTrue(AbstractIdListUtil.isSorted(Arrays.asList(2.3, 3.0, 4L, 5, "a1", "a2", "b1"), true));
        assertFalse(AbstractIdListUtil.isSorted(Arrays.asList(1, 1), true));
        assertFalse(AbstractIdListUtil.isSorted(Arrays.asList("x1", "x1", "x2"), true));
    }

    @Test
    public void testMergeSort() {
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), AbstractIdListUtil.mergeSort(Arrays.asList(1, 2, 5), Arrays.asList(3, 4)));
        assertEquals(Arrays.asList(1, 2, 3, "x", "y"), AbstractIdListUtil.mergeSort(Arrays.asList(1, 2, "x"), Arrays.asList(3, "y")));
        assertEquals(Arrays.asList(1, 2, "x"), AbstractIdListUtil.mergeSort(Arrays.asList(1, 2), Arrays.asList("x")));
        assertEquals(Arrays.asList(1, 2), AbstractIdListUtil.mergeSort(Arrays.asList(1, 2), Arrays.asList()));
    }
}
