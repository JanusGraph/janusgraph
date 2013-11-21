package com.thinkaurelius.titan;

import cern.colt.function.LongObjectProcedure;
import cern.colt.map.AbstractLongObjectMap;
import cern.colt.map.OpenLongObjectHashMap;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

public class TestBed {


    static class A {

        private int c = 0;
        private final Object o;

        A(final Object o) {
            this.o = o;
        }

        public void inc() {
            c++;
        }

    }

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws Exception {
        int size = 100; int trials = 10000; int arrsize = 40;
        Random r = new Random();

        List<byte[]> entries = new ArrayList<byte[]>();
        for (int i=0;i<size;i++) {
            byte[] b = new byte[arrsize];
            for (int j=0;j<b.length;j++) b[j]=(byte)r.nextInt(Byte.MAX_VALUE);
            entries.add(b);
        }

        long time = System.currentTimeMillis();
        for (int i=0;i<trials;i++) {
            int totallength = 0;
            for (byte[] barr : entries) totallength+=barr.length;
            byte[] total = new byte[totallength];
            int[] offsets = new int[entries.size()];
            int pos=0; int index = 0;
            for (byte[] barr : entries) {
                offsets[index++]=pos;
                System.arraycopy(barr,0,total,pos,barr.length);
                pos+=barr.length;
            }
        }
        System.out.println(System.currentTimeMillis()-time);


        System.exit(0);

        Object o = Long.valueOf(5);
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(1).putLong(2).flip();
        time = System.currentTimeMillis();
        for (long i = 0; i < 1000000000l; i++) {
//            A a = new A(o);
//            a.inc();
            ByteBuffer c = bb.duplicate();
            c.get();
        }

        System.out.println("Time: " + (System.currentTimeMillis() - time));

        System.exit(0);


        double[] d = {0.5, 0.2};
        Double[] dd = {new Double(0.6), new Double(0.3)};

        System.out.println(Array.getLength(d));
        System.out.println(((Number) Array.get(d, 1)).doubleValue());
        System.out.println(((Number) Array.get(dd, 1)).doubleValue());

        for (String s : new String[]{"36028797018963978", "5629499534213184", "21392098230009920"}) {
            BigInteger i2 = new BigInteger(s, 10);
            System.out.println(i2.toString(2));
        }


        int[] localPartition = {0, 200};
        ByteBuffer lower = ByteBuffer.allocate(4);
        ByteBuffer upper = ByteBuffer.allocate(4);
        lower.putInt(localPartition[0]);
        upper.putInt(localPartition[1]);
        lower.rewind();
        upper.rewind();


        System.out.println(1 - Integer.MIN_VALUE);
        System.out.println(-2147483647 + Integer.MIN_VALUE);
        System.exit(0);

        byte b = (byte) (15 | (1 << 7));
        System.out.println(b);
        System.out.println(Runtime.getRuntime().maxMemory() / 1024);
        System.out.println(Runtime.getRuntime().totalMemory() / 1024);
        System.out.println(Runtime.getRuntime().freeMemory() / 1024);
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println(memBefore / 1024);
        size = 10000000;
        final int modulo = 7;
        final AbstractLongObjectMap map = new OpenLongObjectHashMap(size);
        for (int i = 1; i <= size; i++) {
            map.put(size, "O" + i);
        }
        time = System.currentTimeMillis();
        map.forEachPair(new LongObjectProcedure() {
            @Override
            public boolean apply(long l, Object o) {
                if (l % modulo == 0) {
                    map.put(l, "T" + l);
                }
                return true;
            }
        });
        System.out.println("Time: " + (System.currentTimeMillis() - time));
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("Memory: " + (memAfter - memBefore) * 1.0 / size);

    }

    public static String toBinary(int b) {
        String res = Integer.toBinaryString(b);
        while (res.length() < 32) res = "0" + res;
        return res;


    }


    private static void codeSnippets() throws Exception {
        TitanGraph g = TitanFactory.open("/tmp/titan");
        g.createKeyIndex("name", Vertex.class);
        Vertex juno = g.addVertex(null);
        juno.setProperty("name", "juno");
        juno = g.getVertices("name", "juno").iterator().next();

        TransactionalGraph tx = g.newTransaction();
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            //threads[i]=new Thread(new DoSomething(tx));
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) threads[i].join();
        tx.commit();
    }

}
