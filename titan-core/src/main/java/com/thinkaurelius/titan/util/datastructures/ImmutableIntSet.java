package com.thinkaurelius.titan.util.datastructures;

import com.google.common.base.Preconditions;

/**
 * Immutable set of integers
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ImmutableIntSet implements IntSet {

    private final int[] values;
    private final int hashcode;

    public ImmutableIntSet(int[] values) {
        Preconditions.checkNotNull(values);
        Preconditions.checkArgument(values.length > 0);
        this.values = values;
        hashcode = ArraysUtil.sum(values);
    }

    public ImmutableIntSet(int value) {
        this(new int[]{value});
    }

    @Override
    public boolean add(int value) {
        throw new UnsupportedOperationException("This IntSet is immutable");
    }

    @Override
    public boolean addAll(int[] values) {
        throw new UnsupportedOperationException("This IntSet is immutable");
    }

    @Override
    public boolean contains(int value) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == value) return true;
        }
        return false;
    }

    @Override
    public int[] getAll() {
        return values;
    }

    @Override
    public boolean remove(int value) {
        throw new UnsupportedOperationException("This IntSet is immutable");
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public int hashCode() {
        return hashcode;
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
