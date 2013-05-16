package com.thinkaurelius.titan.util.stats;

import cern.colt.list.IntArrayList;
import cern.colt.map.AbstractIntObjectMap;
import cern.colt.map.OpenIntObjectHashMap;

import java.io.Serializable;

/**
 * Count absolute integer frequencies
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IntegerLongFrequency implements Serializable {

    private static final long serialVersionUID = -3465555195625674108L;

    private final AbstractIntObjectMap counts;
    private long total;

    public IntegerLongFrequency() {
        counts = new OpenIntObjectHashMap();
        total = 0;
    }

    public void addValue(int value) {
        Object count = counts.get(value);
        if (count == null) {
            count = new Counter();
            counts.put(value, count);
        }
        ((Counter) count).increment();
        total++;
    }

    public IntArrayList getValues() {
        return counts.keys();
    }

    public long getCount(int value) {
        Object count = counts.get(value);
        if (count == null) return 0;
        else return ((Counter) count).count;
    }

    public long getTotal() {
        return total;
    }

    public int getN() {
        return counts.size();
    }


    @SuppressWarnings("serial")
    private class Counter implements Serializable {
        private long count;

        Counter() {
            count = 0;
        }

        void increment() {
            count++;
        }
    }

}
