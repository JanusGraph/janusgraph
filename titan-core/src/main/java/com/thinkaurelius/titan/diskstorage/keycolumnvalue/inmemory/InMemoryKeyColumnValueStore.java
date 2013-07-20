package com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Function;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory implementation of {@link KeyColumnValueStore}.
 * This implementation is thread-safe. All data is held in memory, which means that the capacity of this store is
 * determined by the available heap space. No data is persisted and all data lost when the jvm terminates or store closed.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryKeyColumnValueStore implements KeyColumnValueStore {

    private final String name;
    private final ConcurrentHashMap<StaticBuffer,ColumnValueStore> kcv;

    public InMemoryKeyColumnValueStore(final String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name=name;
        this.kcv = new ConcurrentHashMap<StaticBuffer, ColumnValueStore>();
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
        Preconditions.checkArgument(txh.getConsistencyLevel()==ConsistencyLevel.DEFAULT);
        return new RecordIterator<StaticBuffer>() {

            private final Iterator<StaticBuffer> iter =
                    Iterators.transform(
                    Iterators.filter(kcv.entrySet().iterator(), new Predicate<Map.Entry<StaticBuffer, ColumnValueStore>>() {
                @Override
                public boolean apply(@Nullable Map.Entry<StaticBuffer, ColumnValueStore> entry) {
                    return !entry.getValue().isEmpty(txh);
                }
            }), new Function<Map.Entry<StaticBuffer, ColumnValueStore>, StaticBuffer>() {
                        @Nullable
                        @Override
                        public StaticBuffer apply(@Nullable Map.Entry<StaticBuffer, ColumnValueStore> entry) {
                            return entry.getKey();
                        }
                    });

            @Override
            public boolean hasNext() throws StorageException {
                return iter.hasNext();
            }

            @Override
            public StaticBuffer next() throws StorageException {
                return iter.next();
            }

            @Override
            public void close() throws StorageException {
                //Nothing to do
            }
        };
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


}
