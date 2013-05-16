package com.thinkaurelius.titan.util.datastructures;

import com.google.common.collect.Iterables;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ImmutableLongObjectMapTest {

    private static final Random random = new Random();


    @Test
    public void testMap() {
        int len = 100;
        ImmutableLongObjectMap.Builder b = new ImmutableLongObjectMap.Builder();
        for (int i=1;i<=len;i++) {
            b.put(i*1000,"TestValue " + i);
        }
        ImmutableLongObjectMap map = b.build();

        Map<Long,Object> copy1 = new HashMap<Long,Object>();
        for (int i=0;i<map.size();i++) {
            copy1.put(map.getKey(i), map.getValue(i));
        }
        Map<Long,Object> copy2 = new HashMap<Long,Object>();
        for (ImmutableLongObjectMap.Entry entry : map) {
            copy2.put(entry.getKey(),entry.getValue());
        }
        assertEquals(len,map.size());
        assertEquals(len, copy1.size());
        assertEquals(len, copy2.size());
        for (int i=1;i<=len;i++) {
            assertEquals("TestValue " + i,map.get(i*1000));
            assertEquals("TestValue " + i, copy1.get(i * 1000l));
            assertEquals("TestValue " + i, copy2.get(i * 1000l));
        }

    }

    @Test
    public void testEmpty() {
        ImmutableLongObjectMap.Builder b = new ImmutableLongObjectMap.Builder();
        ImmutableLongObjectMap map = b.build();
        assertEquals(0, map.size());
        assertEquals(0, Iterables.size(map));
    }

    @Test
    public void testPerformance() {
        int trials = 10;
        int iterations = 100000;
        for (int k=0;k<iterations;k++) {
            int len = random.nextInt(10);
            ImmutableLongObjectMap.Builder b = new ImmutableLongObjectMap.Builder();
            for (int i=1;i<=len;i++) {
                b.put(i*1000,"TestValue " + i);
            }
            ImmutableLongObjectMap map = b.build();
            for (int t = 0; t<trials;t++) {
                for (int i=1;i<=len;i++) {
                    assertEquals("TestValue " + i,map.get(i*1000));
                }
                assertEquals(len,map.size());
                for (int i=0;i<map.size();i++) {
                    map.getKey(i); map.getValue(i);
                }
            }
        }
    }


}
