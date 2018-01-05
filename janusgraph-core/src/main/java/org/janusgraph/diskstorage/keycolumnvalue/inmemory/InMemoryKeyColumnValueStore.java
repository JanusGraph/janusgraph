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
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * An in-memory implementation of {@link KeyColumnValueStore}.
 * This implementation is thread-safe. All data is held in memory, which means that the capacity of this store is
 * determined by the available heap space. No data is persisted and all data lost when the jvm terminates or store closed.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryKeyColumnValueStore implements KeyColumnValueStore {

    private final String name;
    private final ConcurrentNavigableMap<StaticBuffer, ColumnValueStore> kcv;

    public InMemoryKeyColumnValueStore(final String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = name;
        this.kcv = new ConcurrentSkipListMap<>();
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        ColumnValueStore cvs = kcv.get(query.getKey());
        if (cvs == null) return EntryList.EMPTY_LIST;
        else return cvs.getSlice(query, txh);
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer,EntryList> result = Maps.newHashMap();
        for (StaticBuffer key : keys) result.put(key,getSlice(new KeySliceQuery(key,query),txh));
        return result;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        ColumnValueStore cvs = kcv.get(key);
        if (cvs == null) {
            kcv.putIfAbsent(key, new ColumnValueStore());
            cvs = kcv.get(key);
        }
        cvs.mutate(additions, deletions, txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        return new RowIterator(kcv.subMap(query.getKeyStart(), query.getKeyEnd()).entrySet().iterator(), query, txh);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return new RowIterator(kcv.entrySet().iterator(), query, txh);
    }

    @Override
    public String getName() {
        return name;
    }

    public void clear() {
        kcv.clear();
    }

    @Override
    public void close() throws BackendException {
        kcv.clear();
    }


    private static class RowIterator implements KeyIterator {
        private final Iterator<Map.Entry<StaticBuffer, ColumnValueStore>> rows;
        private final SliceQuery columnSlice;
        private final StoreTransaction transaction;

        private Map.Entry<StaticBuffer, ColumnValueStore> currentRow;
        private Map.Entry<StaticBuffer, ColumnValueStore> nextRow;
        private boolean isClosed;

        public RowIterator(Iterator<Map.Entry<StaticBuffer, ColumnValueStore>> rows,
                           @Nullable SliceQuery columns,
                           final StoreTransaction transaction) {
            this.rows = Iterators.filter(rows, entry -> entry != null && !entry.getValue().isEmpty(transaction));

            this.columnSlice = columns;
            this.transaction = transaction;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            if (columnSlice == null)
                throw new IllegalStateException("getEntries() requires SliceQuery to be set.");

            final KeySliceQuery keySlice = new KeySliceQuery(currentRow.getKey(), columnSlice);
            return new RecordIterator<Entry>() {
                private final Iterator<Entry> items = currentRow.getValue().getSlice(keySlice, transaction).iterator();

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return items.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return items.next();
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Column removal not supported");
                }
            };
        }

        @Override
        public boolean hasNext() {
            ensureOpen();

            if (null != nextRow)
                return true;

            while (rows.hasNext()) {
                nextRow = rows.next();
                List<Entry> entries = nextRow.getValue().getSlice(new KeySliceQuery(nextRow.getKey(), columnSlice), transaction);
                if (null != entries && 0 < entries.size())
                    break;
            }

            return null != nextRow;
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            Preconditions.checkNotNull(nextRow);

            currentRow = nextRow;
            nextRow = null;

            return currentRow.getKey();
        }

        @Override
        public void close() {
            isClosed = true;
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Key removal not supported");
        }
    }
}
