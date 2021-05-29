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

package org.janusgraph.testutil;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Measures the approximate size of an object in memory, given a Class which
 * has a no-argument constructor.
 */
public final class ObjectSizer {

    interface Factory {
        Object newInstance();
    }

    public static Factory emptyConcurrentHashMap = () -> fill(new ConcurrentHashMap<>(5, 0.75f, 2), 20);

    public static Factory emptyHashMap = () -> fill(new HashMap<>(10, 0.75f), 8);

    public static Factory emptyConcurrentSkipListMap = () -> fill(new ConcurrentSkipListMap<>(), 20);

    public static Factory emptyArrayList = () -> fill(new ArrayList<>(5), 8);

    public static Factory emptyArrayQueue = () -> fill(new ArrayBlockingQueue<>(10), 20);

    public static Factory emptyMultimap = () -> fill(HashMultimap.create(5, 1), 5);

    public static Factory emptyCopyArrayList = () -> fill(new CopyOnWriteArrayList<>(), 20);


    public static Factory stringConcurrentSet = () -> {
        final ConcurrentSkipListSet<String> set = new ConcurrentSkipListSet<>();
        final int size = 100;
        for (int i = 0; i < size; i++) set.add("String" + i);
        return set;
    };

    public static Factory stringConcurrentHashMap = () -> {
        final ConcurrentHashMap<String, Boolean> set = new ConcurrentHashMap<>(1, 1);
        final int size = 1000;
        for (int i = 0; i < size; i++) set.put("String" + i, Boolean.TRUE);
        return set;
    };

    public static Factory emptyConcurrentSkipList = ConcurrentSkipListMap::new;

    public static final Factory guavaFactory = () -> {
        final int size = 10000;
        return CacheBuilder.newBuilder()
                .concurrencyLevel(2).initialCapacity(16*3)
                .maximumSize(10000).<String, Long>build();
    };

    private static class Nothing {}


    public static Map<Integer, Integer> fill(Map<Integer, Integer> m, int size) {
        for (int i = 0; i < size; i++) m.put(i, i);
        return m;
    }

    public static HashMultimap<Integer, Integer> fill(HashMultimap<Integer, Integer> m, int size) {
        for (int i = 0; i < size; i++) m.put(i, i);
        return m;
    }

    public static Collection<Integer> fill(Collection<Integer> m, int size) {
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
            theClass = guavaFactory;
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
        Object[] objects = new Object[SAMPLE_SIZE];
        MemoryAssess mem = new MemoryAssess();
        //build a bunch of identical objects
        try {
            Object throwAway = factory.newInstance();

            mem.start();
            for (int idx = 0; idx < objects.length; ++idx) {
                objects[idx] = factory.newInstance();
            }
            double approximateSize = (mem.end() - 12 - SAMPLE_SIZE*OBJECT_POINTER_SIZE) * 1.0 / SAMPLE_SIZE;
            result = Math.round(approximateSize);
        } catch (Exception ex) {
            System.err.println("Cannot create object using " + factory);
        }
        return result;
    }

    // PRIVATE //
    private static final int SAMPLE_SIZE = 100;

    private static final int OBJECT_POINTER_SIZE = 6; //assuming compressed pointers
}
