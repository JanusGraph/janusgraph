package com.thinkaurelius.titan;

import cern.colt.function.LongObjectProcedure;
import cern.colt.map.AbstractLongObjectMap;
import cern.colt.map.OpenLongObjectHashMap;
import com.google.common.base.Preconditions;
import com.sleepycat.je.util.DbCacheSize;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestBed {

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws Exception {
        int[] localPartition = { 0, 200};
        ByteBuffer lower = ByteBuffer.allocate(4);
        ByteBuffer upper = ByteBuffer.allocate(4);
        lower.putInt(localPartition[0]);
        upper.putInt(localPartition[1]);
        lower.rewind(); upper.rewind();


        System.out.println(1-Integer.MIN_VALUE);
        System.out.println(-2147483647+Integer.MIN_VALUE);
        System.exit(0);

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


    private static void codeSnippets() throws Exception {
        TitanGraph g = TitanFactory.open("/tmp/titan");
        g.createKeyIndex("name",Vertex.class);
        Vertex juno = g.addVertex(null);
        juno.setProperty("name", "juno");
        juno = g.getVertices("name","juno").iterator().next();

        TransactionalGraph tx = g.startTransaction();
        Thread[] threads = new Thread[10];
        for (int i=0;i<threads.length;i++) {
            //threads[i]=new Thread(new DoSomething(tx));
            threads[i].start();
        }
        for (int i=0;i<threads.length;i++) threads[i].join();
        tx.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    }

}
