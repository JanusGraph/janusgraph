package com.thinkaurelius.titan;

import cern.colt.function.LongObjectProcedure;
import cern.colt.map.AbstractLongObjectMap;
import cern.colt.map.OpenLongObjectHashMap;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.FloatSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

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
        NonBlockingHashMapLong<String> id1 = new NonBlockingHashMapLong<String>(128);
        ConcurrentHashMap<Long,String> id2 = new ConcurrentHashMap<Long, String>(128,0.75f,2);


        Cache<String,Long> cache = CacheBuilder.newBuilder().maximumSize(1000).initialCapacity(128).concurrencyLevel(2).build();
        Map<String,Long> map = new ConcurrentHashMap<String, Long>(128,0.75f,2);
        Map<String,Long> map2 = new HashMap<String, Long>(128,0.75f);
        String[] values = RandomGenerator.randomStrings(100,12,13);
        long id = 0;
        for (String v : values) {
            cache.put(v,id);
            map.put(v,id);
            map2.put(v,id);
            id1.put(id,v);
            id2.put(id,v);
            id++;
        }
        Random random = new Random();
        int runs = 10000000;
        for (int t = 0; t < 20; t++) {
            long time = System.currentTimeMillis();
            for (int r = 0; r < runs; r++) {
                long i = random.nextInt(values.length);
                id2.get(i);
            }
            System.out.print((System.currentTimeMillis()-time));
            System.out.print("\t");
            time = System.currentTimeMillis();
            for (int r = 0; r < runs; r++) {
                long i = random.nextInt(values.length);
                id1.get(i);
            }
            System.out.print((System.currentTimeMillis() - time));

//            long time = System.currentTimeMillis();
//            for (int r = 0; r < runs; r++) {
//                String name = values[random.nextInt(values.length)];
//                cache.getIfPresent(name);
//            }
//            System.out.print((System.currentTimeMillis()-time));
//            System.out.print("\t");
//            time = System.currentTimeMillis();
//            for (int r = 0; r < runs; r++) {
//                String name = values[random.nextInt(values.length)];
//                map.get(name);
//            }
//            System.out.print((System.currentTimeMillis() - time));
//            System.out.print("\t");
//            time = System.currentTimeMillis();
//            for (int r = 0; r < runs; r++) {
//                String name = values[random.nextInt(values.length)];
//                map2.get(name);
//            }
//            System.out.print((System.currentTimeMillis()-time));
            System.out.println("");
        }


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
