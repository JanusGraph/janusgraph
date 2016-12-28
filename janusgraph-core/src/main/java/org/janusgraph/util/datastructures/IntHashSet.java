package com.thinkaurelius.titan.util.datastructures;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;

import java.util.Iterator;

/**
 * Implementation of {@link IntSet} against {@link IntIntHashMap}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IntHashSet extends IntIntHashMap implements IntSet {

    private static final long serialVersionUID = -7297353805905443841L;
    private static final int defaultValue = 1;

    public IntHashSet() {
        super();
    }

    public IntHashSet(int size) {
        super(size);
    }

    public boolean add(int value) {
        return super.put(value, defaultValue)==0;
    }

    public boolean addAll(int[] values) {
        boolean addedAll = true;
        for (int i = 0; i < values.length; i++) {
            if (!add(values[i])) addedAll = false;
        }
        return addedAll;
    }

    public boolean contains(int value) {
        return super.containsKey(value);
    }

    public int[] getAll() {
        KeysContainer keys = keys();
        int[] all = new int[keys.size()];
        Iterator<IntCursor> iter = keys.iterator();
        int pos=0;
        while (iter.hasNext()) all[pos++]=iter.next().value;
        return all;
    }


    @Override
    public int size() {
        return super.size();
    }

    @Override
    public int hashCode() {
        return ArraysUtil.sum(getAll());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!(other instanceof IntSet)) return false;
        IntSet oth = (IntSet) other;
        for (int i = 0; i < values.length; i++) {
            if (!oth.contains(values[i])) return false;
        }
        return size() == oth.size();
    }

}
