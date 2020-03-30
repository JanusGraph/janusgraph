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

package org.janusgraph.util.datastructures;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class RelationCacheTest {

    private static final Random random = new Random();


    @Test
    public void testMap() {
        int len = 100;
        final LongObjectHashMap<Object> map = new LongObjectHashMap<>();
        for (int i = 1; i <= len; i++) {
            map.put(i * 1000, "TestValue " + i);
        }

        final Map<Long, Object> copy1 = new HashMap<>();
        for (LongObjectCursor<Object> entry : map) {
            copy1.put(entry.key, entry.value);
        }
        final Map<Long, Object> copy2 = new HashMap<>();
        for (LongObjectCursor<Object> entry : map) {
            copy2.put(entry.key, entry.value);
        }
        assertEquals(len, map.size());
        assertEquals(len, copy1.size());
        assertEquals(len, copy2.size());
        for (int i = 1; i <= len; i++) {
            assertEquals("TestValue " + i, map.get(i * 1000));
            assertEquals("TestValue " + i, copy1.get(i * 1000L));
            assertEquals("TestValue " + i, copy2.get(i * 1000L));
        }

    }

    @Test
    public void testEmpty() {
        final LongObjectHashMap<Object> map = new LongObjectHashMap<>();
        assertEquals(0, map.size());
        assertEquals(0, Iterables.size(map));
    }

    @Test
    public void testPerformance() {
        int trials = 10;
        int iterations = 100000;
        for (int k = 0; k < iterations; k++) {
            int len = random.nextInt(10);
            final LongObjectHashMap<Object> map = new LongObjectHashMap<>();
            for (int i = 1; i <= len; i++) {
                map.put(i * 1000, "TestValue " + i);
            }
            for (int t = 0; t < trials; t++) {
                for (int i = 1; i <= len; i++) {
                    assertEquals("TestValue " + i, map.get(i * 1000));
                }
                assertEquals(len, map.size());
            }
        }
    }


}
