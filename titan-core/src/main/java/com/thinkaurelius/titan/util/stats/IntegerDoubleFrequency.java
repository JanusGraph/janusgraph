package com.thinkaurelius.titan.util.stats;

import cern.colt.list.IntArrayList;
import cern.colt.map.AbstractIntDoubleMap;
import cern.colt.map.OpenIntDoubleHashMap;

/**
 * Count relative integer frequencies
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IntegerDoubleFrequency {

    private final AbstractIntDoubleMap counts;
    private double total;

    public IntegerDoubleFrequency() {
        counts = new OpenIntDoubleHashMap();
        total = 0;
    }

    public void addValue(int value, double amount) {
        counts.put(value, amount + counts.get(value));
        total += amount;
    }

    public IntArrayList getValues() {
        return counts.keys();
    }

    public double getCount(int value) {
        return counts.get(value);
    }

    public double getTotal() {
        return total;
    }

    public int getN() {
        return counts.size();
    }


}
