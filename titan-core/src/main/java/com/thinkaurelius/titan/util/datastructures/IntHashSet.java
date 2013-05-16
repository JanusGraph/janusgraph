package com.thinkaurelius.titan.util.datastructures;

import cern.colt.list.IntArrayList;
import cern.colt.map.OpenIntIntHashMap;

/**
 * Implementation of {@link IntSet} against {@link OpenIntIntHashMap}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IntHashSet extends OpenIntIntHashMap implements IntSet {

    private static final long serialVersionUID = -7297353805905443841L;
    private static final int defaultValue = 1;

    public IntHashSet() {
        super();
    }

    public IntHashSet(int size) {
        super(size);
    }

    public boolean add(int value) {
        return super.put(value, defaultValue);
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

    public boolean remove(int value) {
        return super.removeKey(value);
    }

    public int[] getAll() {
        IntArrayList keys = new IntArrayList(size());
        keys(keys);
        assert keys.size() == size() : keys.size() + " vs " + size();
        int[] all = keys.elements();
        assert all.length == size();
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
