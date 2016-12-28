package com.thinkaurelius.titan.util.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This class counts arbitrary objects of type <K> and tries to do so efficiently in time and space.
 * The class offers methods to increase the count of an object by a specified amount or default 1, as well
 * as retrieving the number of times and object has been counted.
 *
 * @param <K>
 * @author Matthias Broecheler
 */

public class ObjectAccumulator<K extends Object> {

    private HashMap<K, Counter> countMap;

    public ObjectAccumulator() {
        countMap = new HashMap<K, Counter>();
    }

    public ObjectAccumulator(int initialSize) {
        countMap = new HashMap<K, Counter>(initialSize);
    }

    /**
     * Increases the count of object o by inc and returns the new count value
     *
     * @param o
     * @param inc
     * @return
     */
    public double incBy(K o, double inc) {
        Counter c = countMap.get(o);
        if (c == null) {
            c = new Counter();
            countMap.put(o, c);
        }
        c.count += inc;
        return c.count;
    }

    public double getCount(K o) {
        Counter c = countMap.get(o);
        if (c == null) return 0.0;
        else return c.count;
    }

    public double getCountSave(K o) {
        Counter c = countMap.get(o);
        if (c == null) throw new NoSuchElementException("Object [" + o + "] does not exist");
        return c.count;
    }

    public Set<K> getObjects() {
        return countMap.keySet();
    }

    public int numObjects() {
        return countMap.size();
    }

    public boolean hasObject(K obj) {
        return countMap.containsKey(obj);
    }

    public boolean removeObject(K obj) {
        return countMap.remove(obj) != null;
    }

    public K getMaxObject() {
        K result = null;
        double count = Double.MIN_VALUE;
        for (Map.Entry<K,Counter> entry : countMap.entrySet()) {
            if (entry.getValue().count>=count) {
                count=entry.getValue().count;
                result = entry.getKey();
            }
        }
        return result;
    }

    class Counter {
        double count;

        Counter() {
            count = 0;
        }
    }

}
