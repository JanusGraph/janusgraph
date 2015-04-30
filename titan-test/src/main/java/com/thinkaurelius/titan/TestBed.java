package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

public class TestBed {

    static enum TEnum {

        ONE, TWO, THREE {
            @Override
            public String toString() {
                return "three";
            }
        }, FOUR;


        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }

    }


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

    private static final void doSomethingExpensive(int milliseconds) {
        double d=0.0;
        Random r = new Random();
        for (int i=0;i<10000*milliseconds;i++) d+=Math.pow(1.1,r.nextDouble());

    }

    private static final ArrayList<Object> mixedList = new ArrayList<Object>() {{
        add("try1");
        add(2);
    }};

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws Exception {

        Comparator<Integer> lambda = Integer::compare;
        System.out.println(lambda.getClass());

//        System.out.println(TEnum.class);
//        System.out.println(TEnum.ONE.getClass());
//        System.out.println(TEnum.THREE.getClass());
//        System.out.println(TEnum.THREE.getClass().isEnum());
//        System.out.println(TEnum.THREE.getClass().getSuperclass().isEnum());
//        System.out.println(TEnum.THREE instanceof TEnum);
//        System.out.println(Object.class.getSuperclass());


        System.exit(0);

        final ScheduledExecutorService exe = new ScheduledThreadPoolExecutor(1,new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                r.run();
            }
        });
        ScheduledFuture future = exe.scheduleWithFixedDelay(new Runnable() {
            AtomicInteger atomicInt = new AtomicInteger(0);

            @Override
            public void run() {
                try {
                for (int i=0;i<10;i++) {
                    exe.submit(new Runnable() {

                        private final int number = atomicInt.incrementAndGet();

                        @Override
                        public void run() {
                            try {
                                Thread.sleep(150);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            System.out.println(number);
                        }
                    });
                    System.out.println("Submitted: "+i);
//                    doSomethingExpensive(20);
                }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },0,1, TimeUnit.SECONDS);
        Thread.sleep(10000);
//        future.get(1,TimeUnit.SECONDS);
        System.out.println("Cancel: " + future.cancel(false));
        System.out.println("Done: " + future.isDone());
        exe.shutdown();
//        Thread.sleep(2000);
        System.out.println("Terminate: " + exe.awaitTermination(5,TimeUnit.SECONDS));
        System.out.println("DONE");
        NonBlockingHashMapLong<String> id1 = new NonBlockingHashMapLong<String>(128);
        ConcurrentHashMap<Long,String> id2 = new ConcurrentHashMap<Long, String>(128,0.75f,2);



    }

    public static String toBinary(int b) {
        String res = Integer.toBinaryString(b);
        while (res.length() < 32) res = "0" + res;
        return res;


    }

}
