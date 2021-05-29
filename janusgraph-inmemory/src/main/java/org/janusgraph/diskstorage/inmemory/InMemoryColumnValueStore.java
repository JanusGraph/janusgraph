// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.diskstorage.inmemory;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.NoLock;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL;

/**
 * Implements a "row" in the {@link InMemoryKeyColumnValueStore}, which is comprised of
 * column-value pairs. This data is held in a shared sorted array for space and retrieval efficiency, and is paged
 * when/if data size exceeds a threshold to avoid excessive copying on updates.
 */

class InMemoryColumnValueStore {

    static final int DEF_PAGE_SIZE = 500;

    private SharedEntryBuffer buffer;

    public InMemoryColumnValueStore() {
        //we expect most stores to fit into one page, so start with a single-page implementation which has much less overhead
        buffer = new SinglePageEntryBuffer();
    }

    public int getMaxPageSize() {
        return DEF_PAGE_SIZE;
    }

    boolean isEmpty(StoreTransaction txh) {
        Lock lock = getLock(txh);
        lock.lock();
        try {
            return buffer.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    EntryList getSlice(KeySliceQuery query, StoreTransaction txh) {
        Lock lock = getLock(txh);
        lock.lock();
        try {
            return buffer.getSlice(query);
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
            buffer.mutate(add, del, getMaxPageSize());

            if (!buffer.isPaged() && buffer.numEntries() > getMaxPageSize()) {
                //single buffer exceeded max page size - switch to multipage buffer
                //expecting any non-paged buffer implementation to implement BufferPage contract i.e. behave same as a single page
                buffer = new MultiPageEntryBuffer((BufferPage) buffer);
            }
            //NOTE: we could check here if a multi-page buffer was reduced to fit into one page, and switch back to single buffer,
            //however this is unlikely to happen, and would involve copying of all pages into one (similar to defragmentation)
            //so unclear if we need this at all
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

    public int numPages(StoreTransaction txh) {
        this.lock.lock();
        try {
            return buffer.numPages();
        } finally {
            this.lock.unlock();
        }
    }

    public int numEntries(StoreTransaction txh) {
        this.lock.lock();
        try {
            return buffer.numEntries();
        } finally {
            this.lock.unlock();
        }
    }

    public SharedEntryBufferFragmentationReport createFragmentationReport(StoreTransaction txh) {
        this.lock.lock();
        try {
            return buffer.createFragmentationReport(getMaxPageSize());
        } finally {
            this.lock.unlock();
        }
    }

    public void quickDefragment(StoreTransaction txh) {
        this.lock.lock();
        try {
            buffer.quickDefragment(getMaxPageSize());
        } finally {
            lock.unlock();
        }
    }

    public void dumpTo(DataOutputStream out) throws IOException {
        this.lock.lock();
        try {
            buffer.dumpTo(out);
        } finally {
            lock.unlock();
        }
    }

    public static InMemoryColumnValueStore readFrom(DataInputStream in) throws IOException {
        InMemoryColumnValueStore store = new InMemoryColumnValueStore();

        store.buffer = BufferPageUtils.readFrom(in);

        return store;
    }

}
