package com.thinkaurelius.titan.util.stats;


import com.carrotsearch.hppc.IntCollection;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleHashMap;

/**
 * Count relative integer frequencies
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IntegerDoubleFrequency {

    private final IntDoubleMap counts;
    private double total;

    public IntegerDoubleFrequency() {
        counts = new IntDoubleHashMap();
        total = 0;
    }

    public void addValue(int value, double amount) {
        counts.put(value, amount + counts.get(value));
        total += amount;
    }

    public IntCollection getValues() {
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
