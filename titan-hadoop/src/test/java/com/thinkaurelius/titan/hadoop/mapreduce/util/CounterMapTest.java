package com.thinkaurelius.titan.hadoop.mapreduce.util;

import com.thinkaurelius.titan.hadoop.mapreduce.util.CounterMap;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CounterMapTest extends TestCase {

    public void testIncrement() {
        CounterMap<String> map = new CounterMap<String>();
        assertEquals(map.size(), 0);
        map.put("marko", 1l);
        assertEquals(map.size(), 1);
        assertEquals(map.get("marko"), Long.valueOf(1l));

        map.incr("marko", 2l);
        assertEquals(map.size(), 1);
        assertEquals(map.get("marko"), Long.valueOf(3l));

        map.incr("stephen", 2l);
        assertEquals(map.size(), 2);
        assertEquals(map.get("marko"), Long.valueOf(3l));
        assertEquals(map.get("stephen"), Long.valueOf(2l));
    }
}
