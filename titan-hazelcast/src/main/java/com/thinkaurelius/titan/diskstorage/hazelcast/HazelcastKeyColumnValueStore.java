package com.thinkaurelius.titan.diskstorage.hazelcast;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.SignedBytes;
import com.google.common.primitives.UnsignedBytes;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

public class HazelcastKeyColumnValueStore implements KeyColumnValueStore {
    private final MultiMap<byte[], Column> cache;

    public HazelcastKeyColumnValueStore(String name, HazelcastInstance manager) {
        this.cache = manager.getMultiMap(name);
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return cache.containsKey(key.as(StaticArrayBuffer.ARRAY_FACTORY));
    }

    @Override
    public List<Entry> getSlice(final KeySliceQuery query, StoreTransaction txh) throws StorageException {
        int count = 0;
        List<Entry> results = new ArrayList<Entry>();
        List<Column> columns = Lists.newArrayList(cache.get(query.getKey().as(StaticArrayBuffer.ARRAY_FACTORY)));

        Collections.sort(columns, new Comparator<Column>() {
            private final Comparator<byte[]> byteComparator = SignedBytes.lexicographicalComparator();

            @Override
            public int compare(Column a, Column b) {
                return byteComparator.compare(a.name, b.name);
            }
        });

        for (Column column : columns) {
            if (query.hasLimit() && count >= query.getLimit())
                break;

            StaticBuffer name = new StaticArrayBuffer(column.name);

            if (name.compareTo(query.getSliceStart()) < 0)
                continue;

            if (name.compareTo(query.getSliceEnd()) >= 0)
                break;

            results.add(new StaticBufferEntry(new StaticArrayBuffer(column.name), new StaticArrayBuffer(column.value)));
            count++;
        }

        return results;
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        List<List<Entry>> result = new ArrayList<List<Entry>>();

        for (StaticBuffer key : keys) {
            result.add(getSlice(new KeySliceQuery(key, query), txh));
        }

        return result;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        for (StaticBuffer columnToDelete : deletions) {
            for (Column column : cache.get(key.as(StaticArrayBuffer.ARRAY_FACTORY))) {
                if (new StaticArrayBuffer(column.name).equals(columnToDelete)) {
                    cache.remove(key.as(StaticArrayBuffer.ARRAY_FACTORY), column);
                }
            }
        }

        for (Entry addition : additions) {
            cache.put(key.as(StaticArrayBuffer.ARRAY_FACTORY), new Column(addition.getColumn(), addition.getValue()));
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        // locking is not supported
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        final Iterator<byte[]> entries = Iterators.filter(cache.keySet().iterator(), new Predicate<byte[]>() {
            @Override
            public boolean apply(@Nullable byte[] rawKey) {
                StaticBuffer key = new StaticArrayBuffer(rawKey);
                return key.compareTo(query.getKeyStart()) >= 0 && key.compareTo(query.getKeyEnd()) < 0;
            }
        });

        return new HazelcatKeyIterator(entries, query);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        return new HazelcatKeyIterator(cache.keySet().iterator(), query);
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return cache.getName();
    }

    @Override
    public void close() throws StorageException {
        cache.destroy();
    }

    public static class Column implements DataSerializable {
        public byte[] name, value;

        @SuppressWarnings("unused") // required by Hazelcast
        public Column() {}

        public Column(StaticBuffer name, StaticBuffer value) {
            this.name  = name.as(StaticArrayBuffer.ARRAY_FACTORY);
            this.value = value.as(StaticArrayBuffer.ARRAY_FACTORY);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(name.length);
            out.write(name);
            out.writeInt(value.length);
            out.write(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = new byte[in.readInt()];
            in.readFully(name);
            value = new byte[in.readInt()];
            in.readFully(value);
        }
    }

    public class HazelcatKeyIterator implements KeyIterator {
        private final Iterator<byte[]> keys;
        private final SliceQuery query;

        private byte[] currentKey;
        private boolean isClosed;

        public HazelcatKeyIterator(Iterator<byte[]> keys, SliceQuery query) {
            this.keys  = keys;
            this.query = query;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private int count = 0;

                private final Iterator<Column> columns = Iterators.filter(cache.get(currentKey).iterator(), new Predicate<Column>() {
                    @Override
                    public boolean apply(@Nullable Column column) {
                        if (column == null)
                            return false;

                        StaticBuffer name = new StaticArrayBuffer(column.name);
                        return name.compareTo(query.getSliceStart()) >= 0 && name.compareTo(query.getSliceEnd()) < 0 && count < query.getLimit();
                    }
                });

                @Override

                public boolean hasNext() throws StorageException {
                    ensureOpen();
                    return columns.hasNext();
                }

                @Override
                public Entry next() throws StorageException {
                    ensureOpen();
                    Column column = columns.next();
                    count++;
                    return new StaticBufferEntry(new StaticArrayBuffer(column.name), new StaticArrayBuffer(column.value));
                }

                @Override
                public void close() throws StorageException {
                    // nothing to do here
                }
            };
        }

        @Override
        public boolean hasNext() throws StorageException {
            ensureOpen();
            return keys.hasNext();
        }

        @Override
        public StaticBuffer next() throws StorageException {
            ensureOpen();

            currentKey = keys.next();
            return new StaticArrayBuffer(currentKey);
        }

        @Override
        public void close() throws StorageException {
            isClosed = true;
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("iterator is closed.");
        }
    }
}
