package com.thinkaurelius.titan.testutil;

import cern.colt.map.AbstractIntIntMap;
import cern.colt.map.OpenIntIntHashMap;
import com.google.common.collect.HashMultimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Measures the approximate size of an object in memory, given a Class which
 * has a no-argument constructor.
 */
public final class ObjectSizer {

    interface Factory {
        public Object newInstance();
    }

    public static Factory emptyConcurrentHashMap = new Factory() {
        @Override
        public Object newInstance() {
            return fill(new ConcurrentHashMap(5, 0.75f, 2), 20);
        }
    };

    public static Factory emptyHashMap = new Factory() {
        @Override
        public Object newInstance() {
            return fill(new HashMap(10, 0.75f), 8);
        }
    };

    public static Factory emptyConcurrentSkipListMap = new Factory() {
        @Override
        public Object newInstance() {
            return fill(new ConcurrentSkipListMap(), 20);
        }
    };

    public static Factory emptyArrayList = new Factory() {
        @Override
        public Object newInstance() {
            return fill(new ArrayList(5), 8);
        }
    };

    public static Factory emptyArrayQueue = new Factory() {
        @Override
        public Object newInstance() {
            return fill(new ArrayBlockingQueue(10), 20);
        }
    };

    public static Factory emptyMultimap = new Factory() {
        @Override
        public Object newInstance() {
            return fill(HashMultimap.create(5, 1), 5);
        }
    };

//	public static Factory emptyArrayAdjacency = new Factory() {
//		@Override
//		public Object newInstance() { return new ArrayAdjacencyList(); }
//	};

    public static Factory emptyCopyArrayList = new Factory() {
        @Override
        public Object newInstance() {
            return fill(new CopyOnWriteArrayList(), 20);
        }
    };

    public static Factory emptyIntIntMap = new Factory() {
        @Override
        public Object newInstance() {
            AbstractIntIntMap m = new OpenIntIntHashMap(5);
            int size = 3;
            for (int i = 0; i < size; i++) m.put(i, i);
            return m;
        }
    };

    public static Factory stringConcurrentSet = new Factory() {
        @Override
        public Object newInstance() {
            ConcurrentSkipListSet<String> set = new ConcurrentSkipListSet<String>();
            int size = 100;
            for (int i = 0; i < size; i++) set.add("String" + i);
            return set;
        }
    };

    public static Factory stringConcurrenthashmap = new Factory() {
        @Override
        public Object newInstance() {
            ConcurrentHashMap<String, Boolean> set = new ConcurrentHashMap<String, Boolean>(1, 1);
            int size = 1000;
            for (int i = 0; i < size; i++) set.put("String" + i, Boolean.TRUE);
            return set;
        }
    };

    public static Factory emptyConcurrentSkipList = new Factory() {
        @Override
        public Object newInstance() {
            ConcurrentSkipListMap<String, String> map = new ConcurrentSkipListMap<String, String>();
            return map;
        }
    };


    public static Map fill(Map m, int size) {
        for (int i = 0; i < size; i++) m.put(i, i);
        return m;
    }

    public static HashMultimap fill(HashMultimap m, int size) {
        for (int i = 0; i < size; i++) m.put(i, i);
        return m;
    }

    public static Collection fill(Collection m, int size) {
        for (int i = 0; i < size; i++) m.add(i);
        return m;
    }

    /**
     * First and only argument is the package-qualified name of a class
     * which has a no-argument constructor.
     */
    public static void main(String... aArguments) {
        Factory theClass = null;
        try {
            theClass = stringConcurrenthashmap;
        } catch (Exception ex) {
            System.err.println("Cannot build a Class object: " + aArguments[0]);
            System.err.println("Use a package-qualified name, and check classpath.");
        }
        long size = ObjectSizer.getObjectSize(theClass);
        System.out.println("Approximate size of " + theClass + " objects :" + size);
    }


    /**
     * Return the approximate size in bytes, and return zero if the class
     * has no default constructor.
     */
    public static long getObjectSize(Factory factory) {
        long result = 0;

        //this array will simply hold a bunch of references, such that
        //the objects cannot be garbage-collected
        Object[] objects = new Object[fSAMPLE_SIZE];
        MemoryAssess mem = new MemoryAssess();
        //build a bunch of identical objects
        try {
            Object throwAway = factory.newInstance();

            mem.start();
            for (int idx = 0; idx < objects.length; ++idx) {
                objects[idx] = factory.newInstance();
            }
            float approximateSize = mem.end() / 100f;
            result = Math.round(approximateSize);
        } catch (Exception ex) {
            System.err.println("Cannot create object using " + factory);
        }
        return result;
    }

    // PRIVATE //
    private static int fSAMPLE_SIZE = 100;


    private static long fSLEEP_INTERVAL = 100;

    private static long getMemoryUse() {
        putOutTheGarbage();
        long totalMemory = Runtime.getRuntime().totalMemory();

        putOutTheGarbage();
        long freeMemory = Runtime.getRuntime().freeMemory();

        return (totalMemory - freeMemory);
    }

    private static void putOutTheGarbage() {
        collectGarbage();
        collectGarbage();
    }

    private static void collectGarbage() {
        try {
            System.gc();
            Thread.currentThread().sleep(fSLEEP_INTERVAL);
            System.runFinalization();
            Thread.currentThread().sleep(fSLEEP_INTERVAL);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
} 
