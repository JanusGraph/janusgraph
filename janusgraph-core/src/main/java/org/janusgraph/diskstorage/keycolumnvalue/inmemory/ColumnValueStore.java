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

package org.janusgraph.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.NoLock;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL;

/**
 * Implements a row in the in-memory implementation {@link InMemoryKeyColumnValueStore} which is comprised of
 * column-value pairs. This data is held in a sorted array for space and retrieval efficiency.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

class ColumnValueStore {

    private static final double SIZE_THRESHOLD = 0.66;

    private Data data;

    public ColumnValueStore() {
        data = new Data(new Entry[0], 0);
    }

    boolean isEmpty(StoreTransaction txh) {
        Lock lock = getLock(txh);
        lock.lock();
        try {
            return data.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    EntryList getSlice(KeySliceQuery query, StoreTransaction txh) {
        Lock lock = getLock(txh);
        lock.lock();
        try {
            Data datacp = data;
            int start = datacp.getIndex(query.getSliceStart());
            if (start < 0) start = (-start - 1);
            int end = datacp.getIndex(query.getSliceEnd());
            if (end < 0) end = (-end - 1);
            if (start < end) {
                MemoryEntryList result = new MemoryEntryList(end - start);
                for (int i = start; i < end; i++) {
                    if (query.hasLimit() && result.size() >= query.getLimit()) break;
                    result.add(datacp.get(i));
                }
                return result;
            } else {
                return EntryList.EMPTY_LIST;
            }
        } finally {
            lock.unlock();
        }
    }

    private static class MemoryEntryList extends ArrayList<Entry> implements EntryList {

        public MemoryEntryList(int size) {
            super(size);
        }

        @Override
        public Iterator<Entry> reuseIterator() {
            return iterator();
        }

        @Override
        public int getByteSize() {
            int size = 48;
            for (Entry e : this) {
                size += 8 + 16 + 8 + 8 + e.length();
            }
            return size;
        }
    }


    synchronized void mutate(List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) {
        //Prepare data
        Entry[] add;
        if (!additions.isEmpty()) {
            add = new Entry[additions.size()];
            int pos = 0;
            for (Entry e : additions) {
                add[pos] = e;
                pos++;
            }
            Arrays.sort(add);
        } else add = new Entry[0];

        //Filter out deletions that are also added
        Entry[] del;
        if (!deletions.isEmpty()) {
            del = new Entry[deletions.size()];
            int pos=0;
            for (StaticBuffer deletion : deletions) {
                Entry delEntry = StaticArrayEntry.of(deletion);
                if (Arrays.binarySearch(add,delEntry) >= 0) continue;
                del[pos++]=delEntry;
            }
            if (pos<deletions.size()) del = Arrays.copyOf(del,pos);
            Arrays.sort(del);
        } else del = new Entry[0];

        Lock lock = getLock(txh);
        lock.lock();
        try {
            Entry[] oldData = data.array;
            int oldSize = data.size;
            Entry[] newData = new Entry[oldSize + add.length];

            //Merge sort
            int i = 0, indexOld = 0, indexAdd = 0, indexDelete = 0;
            while (indexOld < oldSize) {
                Entry e = oldData[indexOld];
                indexOld++;
                //Compare with additions
                if (indexAdd < add.length) {
                    int compare = e.compareTo(add[indexAdd]);
                    if (compare >= 0) {
                        e = add[indexAdd];
                        indexAdd++;
                        //Skip duplicates
                        while (indexAdd < add.length && e.equals(add[indexAdd])) indexAdd++;
                    }
                    if (compare > 0) indexOld--;
                }
                //Compare with deletions
                if (indexDelete < del.length) {
                    int compare = e.compareTo(del[indexDelete]);
                    if (compare == 0) e = null;
                    if (compare >= 0) indexDelete++;
                }
                if (e != null) {
                    newData[i] = e;
                    i++;
                }
            }
            while (indexAdd < add.length) {
                newData[i] = add[indexAdd];
                i++;
                indexAdd++;
            }

            if (i * 1.0 / newData.length < SIZE_THRESHOLD) {
                //shrink array to free space
                Entry[] tempData = newData;
                newData = new Entry[i];
                System.arraycopy(tempData, 0, newData, 0, i);
            }
            data = new Data(newData, i);
        } finally {
            lock.unlock();
        }
    }

    private volatile ReentrantLock lock = null;

    private Lock getLock(StoreTransaction txh) {
        Boolean txOn = txh.getConfiguration().getCustomOption(STORAGE_TRANSACTIONAL);
        if (null != txOn && txOn) {
            ReentrantLock result = lock;
            if (result == null) {
                synchronized (this) {
                    result = lock;
                    if (result == null) {
                        lock = result = new ReentrantLock();
                    }
                }
            }
            return result;
        } else return NoLock.INSTANCE;
    }

    private static class Data {

        final Entry[] array;
        final int size;

        Data(final Entry[] array, final int size) {
            Preconditions.checkArgument(size >= 0 && size <= array.length);
            assert isSorted();
            this.array = array;
            this.size = size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        int getIndex(StaticBuffer column) {
            return Arrays.binarySearch(array, 0, size, StaticArrayEntry.of(column));
        }

        Entry get(int index) {
            return array[index];
        }

        boolean isSorted() {
            for (int i = 1; i < size; i++) {
                if (!(array[i].compareTo(array[i - 1]) > 0)) return false;
            }
            return true;
        }

    }


}
