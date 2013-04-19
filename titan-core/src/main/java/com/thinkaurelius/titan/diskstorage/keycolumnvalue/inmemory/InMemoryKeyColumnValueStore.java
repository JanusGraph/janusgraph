package com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.NoLock;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryKeyColumnValueStore implements KeyColumnValueStore {

    private final String name;
    private final ConcurrentHashMap<ByteBuffer,ColumnValueStore> kcv;

    public InMemoryKeyColumnValueStore(final String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name=name;
        this.kcv = new ConcurrentHashMap<ByteBuffer, ColumnValueStore>();
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
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
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        ColumnValueStore cvs = kcv.get(key);
        if (cvs==null) return null;
        else return cvs.get(column,txh);
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return get(key,column,txh)!=null;
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        ColumnValueStore cvs = kcv.get(key);
        if (cvs==null) {
            kcv.putIfAbsent(key,new ColumnValueStore());
            cvs = kcv.get(key);
        }
        cvs.mutate(additions,deletions,txh);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        Preconditions.checkArgument(txh.getConsistencyLevel()==ConsistencyLevel.DEFAULT);
        return new RecordIterator<ByteBuffer>() {

            private final Iterator<ByteBuffer> iter = kcv.keySet().iterator();

            @Override
            public boolean hasNext() throws StorageException {
                return iter.hasNext();
            }

            @Override
            public ByteBuffer next() throws StorageException {
                return iter.next();
            }

            @Override
            public void close() throws StorageException {
                //Nothing to do
            }
        };
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws StorageException {
        kcv.clear();
    }


}
