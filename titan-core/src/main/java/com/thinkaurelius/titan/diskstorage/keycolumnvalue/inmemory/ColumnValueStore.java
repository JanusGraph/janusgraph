package com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.NoLock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) {
        Lock lock = getLock(txh);
        lock.lock();
        try {
            Data datacp = data;
            int start = datacp.getIndex(query.getSliceStart());
            if (start < 0) start = (-start - 1);
            int end = datacp.getIndex(query.getSliceEnd());
            if (end < 0) end = (-end - 1);
            if (start < end) {
                List<Entry> result = new ArrayList<Entry>(end - start);
                for (int i = start; i < end; i++) {
                    if (query.hasLimit() && result.size() >= query.getLimit()) break;
                    result.add(datacp.get(i));
                }
                return result;
            } else {
                return ImmutableList.of();
            }
        } finally {
            lock.unlock();
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
        StaticBuffer[] del;
        if (!deletions.isEmpty()) {
            Iterator<StaticBuffer> iter = deletions.iterator();
            while (iter.hasNext()) {
                if (Arrays.binarySearch(add, new StaticBufferEntry(iter.next(), null)) >= 0) {
                    iter.remove();
                }
            }
            del = deletions.toArray(new StaticBuffer[deletions.size()]);
            Arrays.sort(del);
        } else del = new StaticBuffer[0];

        Lock lock = getLock(txh);
        lock.lock();
        try {
            Entry[] olddata = data.array;
            int oldsize = data.size;
            Entry[] newdata = new Entry[oldsize + add.length];

            //Merge sort
            int i = 0, iold = 0, iadd = 0, idel = 0;
            while (iold < oldsize) {
                Entry e = olddata[iold];
                iold++;
                //Compare with additions
                if (iadd < add.length) {
                    int compare = e.compareTo(add[iadd]);
                    if (compare >= 0) {
                        e = add[iadd];
                        iadd++;
                        //Skip duplicates
                        while (iadd < add.length && e.equals(add[iadd])) iadd++;
                    }
                    if (compare > 0) iold--;
                }
                //Compare with deletions
                if (idel < del.length) {
                    int compare = ByteBufferUtil.compare(e.getColumn(), del[idel]);
                    if (compare == 0) e = null;
                    if (compare >= 0) idel++;
                }
                if (e != null) {
                    newdata[i] = e;
                    i++;
                }
            }
            while (iadd < add.length) {
                newdata[i] = add[iadd];
                i++;
                iadd++;
            }

            if (i * 1.0 / newdata.length < SIZE_THRESHOLD) {
                //shrink array to free space
                Entry[] tmpdata = newdata;
                newdata = new Entry[i];
                System.arraycopy(tmpdata, 0, newdata, 0, i);
            }
            data = new Data(newdata, i);
        } finally {
            lock.unlock();
        }
    }

    private ReentrantLock lock = null;

    private Lock getLock(StoreTransaction txh) {
        if (txh.getConfiguration().getConsistency() == ConsistencyLevel.KEY_CONSISTENT) {
            if (lock == null) {
                synchronized (this) {
                    if (lock == null) {
                        lock = new ReentrantLock();
                    }
                }
            }
            return lock;
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
            return Arrays.binarySearch(array, 0, size, new StaticBufferEntry(column, null));
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
