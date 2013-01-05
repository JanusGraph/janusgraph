package com.thinkaurelius.titan.util.datastructures;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BitMapTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testBitMap() {
        byte map = BitMap.setBitb(BitMap.createMapb(2), 4);
        assertTrue(BitMap.readBitb(map, 2));
        assertTrue(BitMap.readBitb(map, 4));
        map = BitMap.unsetBitb(map, 2);
        assertFalse(BitMap.readBitb(map, 2));
        assertFalse(BitMap.readBitb(map, 3));
        assertFalse(BitMap.readBitb(map, 7));
        map = BitMap.setBitb(map, 7);
        assertTrue(BitMap.readBitb(map, 7));
    }

    @After
    public void tearDown() throws Exception {
    }

}
