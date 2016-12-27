package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class EntryArrayList extends ArrayList<Entry> implements EntryList {

    public static final int ENTRY_SIZE_ESTIMATE = 256;

    public EntryArrayList() {
        super();
    }

    public EntryArrayList(Collection<? extends Entry> c) {
        super(c);
    }

    public static EntryArrayList of(Iterable<? extends Entry> i) {
        // This is adapted from Guava's Lists.newArrayList implementation
        EntryArrayList result;
        if (i instanceof Collection) {
            // Let ArrayList's sizing logic work, if possible
            result = new EntryArrayList((Collection)i);
        } else {
            // Unknown size
            result = new EntryArrayList();
            Iterators.addAll(result, i.iterator());
        }
        return result;
    }

    @Override
    public Iterator<Entry> reuseIterator() {
        return iterator();
    }

    /**
     * This implementation is an inexact estimate.
     * It's just the product of a constant ({@link #ENTRY_SIZE_ESTIMATE} times the array size.
     * The exact size could be calculated by iterating over the list and summing the remaining
     * size of each StaticBuffer in each Entry.
     *
     * @return crude approximation of actual size
     */
    @Override
    public int getByteSize() {
        return size() * ENTRY_SIZE_ESTIMATE;
    }
}
