package com.thinkaurelius.titan;

import cern.colt.function.LongObjectProcedure;
import cern.colt.map.AbstractLongObjectMap;
import cern.colt.map.OpenLongObjectHashMap;
import com.sleepycat.je.util.DbCacheSize;

import java.io.IOException;

public class TestBed {

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException {
		byte b = (byte)(15 | (1<<7));
        System.out.println(b);
        System.out.println(Runtime.getRuntime().maxMemory()/1024);
        System.out.println(Runtime.getRuntime().totalMemory()/1024);
        System.out.println(Runtime.getRuntime().freeMemory()/1024);
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println(memBefore/1024);
        int size = 10000000;
        final int modulo = 7;
        final AbstractLongObjectMap map = new OpenLongObjectHashMap(size);
        for (int i=1;i<=size;i++) {
            map.put(size,"O"+i);
        }
        long time = System.currentTimeMillis();
        map.forEachPair(new LongObjectProcedure() {
            @Override
            public boolean apply(long l, Object o) {
                if (l%modulo==0) {
                    map.put(l,"T"+l);
                }
                return true;
            }
        });        
        System.out.println("Time: " + (System.currentTimeMillis()-time));
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("Memory: " + (memAfter-memBefore)*1.0/size);
        
	}

	public static String toBinary(int b) {
		String res = Integer.toBinaryString(b);
		while (res.length()<32) res = "0" + res;
		return res;
		

	}
	

}
