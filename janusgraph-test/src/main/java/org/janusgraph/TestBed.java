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

package org.janusgraph;

import javax.annotation.Nonnull;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestBed {

    enum TEnum {

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

    public static class B {

        public B(int a) {}

    }

    private static void doSomethingExpensive(int milliseconds) {
        double d=0.0;
        Random r = new Random();
        for (int i=0;i<10000*milliseconds;i++) d+=Math.pow(1.1,r.nextDouble());

    }

    public static class TestA {

        final Number a;
        final String b;

        public TestA(Number a, String b) {
            this.a = a;
            this.b = b;
        }
    }

    interface Observer {

        boolean doesNotObserve();

        void observe(Object o);

        Observer NO_OP = new Observer() {

            @Override
            public boolean doesNotObserve() {
                return true;
            }

            @Override
            public void observe(Object o) {
                System.out.println(o);
            }
        };

    }

    static class ObserverManager implements Observer {

        private final boolean observed = false;

        public void observe(Object o, Observer other) {
            if (other.doesNotObserve()) return;
            observe(o);
            other.observe(o);
        }

        public void observe(Object o1, Object o2, Observer other) {
            if (other.doesNotObserve()) return;
            final List<Object> combined = new ArrayList<>();
            combined.add(o1);
            combined.add(o2);
            observe(combined);
            other.observe(combined);
        }

        public boolean doesNotObserve() {
            return !observed;
        }

        @Override
        public void observe(Object o) {
            System.out.println(o);
        }

    }

    static class Context {

        private final ObserverManager om;
        private final Observer os;

        Context(ObserverManager om, Observer os) {
            this.om = om;
            this.os = os;
        }

        public void observe(Object o) {
            om.observe(o,os);
        }

        public void observe(Object o1, Object o2) {
            om.observe(o1,o2,os);
        }


    }

    static class Container {
        private final String s1;
        private final String s2;

        Container(String s1, String s2) {
            this.s1 = s1;
            this.s2 = s2;
        }
    }

    public static @Nonnull String getInt(@Nonnull int a, int b) {
        return String.valueOf(a+b);
    }


    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws Exception {
        Method method = TestBed.class.getMethod("getInt",int.class,int.class);
        AnnotatedType rt = method.getAnnotatedReturnType();
        System.out.println(rt.getType());
        System.out.println(rt.getAnnotations().length);
        System.out.println(method.getAnnotations().length);
        for (int i = 0; i < method.getAnnotations().length; i++) {
            System.out.println(method.getAnnotations()[i]);
        }


//        String[] s = {"a","b","c","d","e","f","g","h","i","x","u"};
//        int len = s.length;
//        Random random = new Random();
//
//        Context c = new Context(new ObserverManager(),Observer.NO_OP);
//        //Warmup
//        for (int i = 0; i < 1000000000; i++) {
//            c.observe(s[1],s[2]);
//        }
//        long before = System.nanoTime();
//        for (int i = 0; i < 1000000000; i++) {
//            c.observe(s[1],s[2]);
//        }
//        long total = System.nanoTime()-before;
//        System.out.println("Total time: " + total/1000000);

        System.exit(0);

        final ScheduledExecutorService exe = new ScheduledThreadPoolExecutor(1, (r, executor) -> r.run());
        ScheduledFuture future = exe.scheduleWithFixedDelay(new Runnable() {
            final AtomicInteger atomicInt = new AtomicInteger(0);

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
    }

    public static String toBinary(int b) {
        final StringBuilder res = new StringBuilder(Integer.toBinaryString(b));
        while (res.length() < 32) res.insert(0, "0");
        return res.toString();
    }

}
