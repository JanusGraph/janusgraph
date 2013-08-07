package com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
    private final ConcurrentNavigableMap<StaticBuffer,ColumnValueStore> kcv;

    public InMemoryKeyColumnValueStore(final String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = name;
        this.kcv = new ConcurrentSkipListMap<StaticBuffer, ColumnValueStore>();
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        ColumnValueStore cvs = kcv.get(key);
        return cvs!=null && !cvs.isEmpty(txh);
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        ColumnValueStore cvs = kcv.get(query.getKey());
        if (cvs==null) return Lists.newArrayList();
        else return cvs.getSlice(query,txh);
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        List<List<Entry>> results = new ArrayList<List<Entry>>();

        for (StaticBuffer key : keys) {
            results.add(getSlice(new KeySliceQuery(key, query), txh));
        }

        return results;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        ColumnValueStore cvs = kcv.get(key);
        if (cvs==null) {
            kcv.putIfAbsent(key,new ColumnValueStore());
            cvs = kcv.get(key);
        }
        cvs.mutate(additions,deletions,txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<StaticBuffer> getKeys(final StoreTransaction txh) throws StorageException {
        Preconditions.checkArgument(txh.getConsistencyLevel() == ConsistencyLevel.DEFAULT);
        return new RowIterator(kcv.entrySet().iterator(), null, txh);
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws StorageException {
        Preconditions.checkArgument(txh.getConsistencyLevel() == ConsistencyLevel.DEFAULT);
        return new RowIterator(kcv.subMap(query.getKeyStart(), query.getKeyEnd()).entrySet().iterator(), query, txh);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        Preconditions.checkArgument(txh.getConsistencyLevel() == ConsistencyLevel.DEFAULT);
        return new RowIterator(kcv.entrySet().iterator(), query, txh);
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    public void clear() {
        kcv.clear();
    }

    @Override
    public void close() throws StorageException {
        kcv.clear();
    }


    private static class RowIterator implements KeyIterator {
        private final Iterator<Map.Entry<StaticBuffer, ColumnValueStore>> rows;
        private final SliceQuery columnSlice;
        private final StoreTransaction transaction;

        private Map.Entry<StaticBuffer, ColumnValueStore> currentRow;
        private boolean isClosed;

        public RowIterator(Iterator<Map.Entry<StaticBuffer, ColumnValueStore>> rows,
                           @Nullable SliceQuery columns,
                           final StoreTransaction transaction) {
            this.rows = Iterators.filter(rows, new Predicate<Map.Entry<StaticBuffer, ColumnValueStore>>() {
                @Override
                public boolean apply(@Nullable Map.Entry<StaticBuffer, ColumnValueStore> entry) {
                    return entry != null && !entry.getValue().isEmpty(transaction);
                }
            });

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
                public boolean hasNext() throws StorageException {
                    ensureOpen();
                    return items.hasNext();
                }

                @Override
                public Entry next() throws StorageException {
                    ensureOpen();
                    return items.next();
                }

                @Override
                public void close() throws StorageException {
                    isClosed = true;
                }
            };
        }

        @Override
        public boolean hasNext() throws StorageException {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() throws StorageException {
            ensureOpen();
            currentRow = rows.next();
            return currentRow.getKey();
        }

        @Override
        public void close() throws StorageException {
            isClosed = true;
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }
    }
}
