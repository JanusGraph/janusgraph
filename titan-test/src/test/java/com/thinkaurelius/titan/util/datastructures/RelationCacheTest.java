package com.thinkaurelius.titan.util.datastructures;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class RelationCacheTest {

    private static final Random random = new Random();


    @Test
    public void testMap() {
        int len = 100;
        LongObjectOpenHashMap<Object> map = new LongObjectOpenHashMap<Object>();
        for (int i = 1; i <= len; i++) {
            map.put(i * 1000, "TestValue " + i);
        }

        Map<Long, Object> copy1 = new HashMap<Long, Object>();
        for (LongObjectCursor<Object> entry : map) {
            copy1.put(entry.key, entry.value);
        }
        Map<Long, Object> copy2 = new HashMap<Long, Object>();
        for (LongObjectCursor<Object> entry : map) {
            copy2.put(entry.key, entry.value);
        }
        assertEquals(len, map.size());
        assertEquals(len, copy1.size());
        assertEquals(len, copy2.size());
        for (int i = 1; i <= len; i++) {
            assertEquals("TestValue " + i, map.get(i * 1000));
            assertEquals("TestValue " + i, copy1.get(i * 1000l));
            assertEquals("TestValue " + i, copy2.get(i * 1000l));
        }

    }

    @Test
    public void testEmpty() {
        LongObjectOpenHashMap<Object> map = new LongObjectOpenHashMap<Object>();
        assertEquals(0, map.size());
        assertEquals(0, Iterables.size(map));
    }

    @Test
    public void testPerformance() {
        int trials = 10;
        int iterations = 100000;
        for (int k = 0; k < iterations; k++) {
            int len = random.nextInt(10);
            LongObjectOpenHashMap<Object> map = new LongObjectOpenHashMap<Object>();
            for (int i = 1; i <= len; i++) {
                map.put(i * 1000, "TestValue " + i);
            }
            for (int t = 0; t < trials; t++) {
                for (int i = 1; i <= len; i++) {
                    assertEquals("TestValue " + i, map.get(i * 1000));
                }
                assertEquals(len, map.size());
                for (LongObjectCursor<Object> entry : map) {
                }
            }
        }
    }


}
