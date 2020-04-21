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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;

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

    public static EntryArrayList of(Iterator<? extends Entry> i) {
        EntryArrayList result = new EntryArrayList();
        i.forEachRemaining(result::add);
        return result;
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
            i.forEach(result::add);
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
