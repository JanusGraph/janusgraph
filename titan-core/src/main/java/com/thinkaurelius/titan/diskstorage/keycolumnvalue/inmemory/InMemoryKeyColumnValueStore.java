package com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryKeyColumnValueStore implements KeyColumnValueStore {

    private static final ByteBuffer ZERO = ByteBufferUtil.zeroByteBuffer(8);

    private final String name;
    private final ConcurrentSkipListMap<KeyColumn,ByteBuffer> kcv;
    private final ConcurrentHashMap<ByteBuffer,Lock> keyLocks;

    public InMemoryKeyColumnValueStore(final String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name=name;
        this.kcv = new ConcurrentSkipListMap<KeyColumn, ByteBuffer>();
        this.keyLocks = new ConcurrentHashMap<ByteBuffer, Lock>();
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        KeyColumn start = new KeyColumn(key,ZERO);
        KeyColumn end = new KeyColumn(ByteBufferUtil.nextBiggerBuffer(key),ZERO);
        Lock lock = getLock(key,txh);
        try {
            lock.lock();
            return !kcv.subMap(start,end).isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        KeyColumn start = new KeyColumn(query.getKey(),query.getSliceStart());
        KeyColumn end = new KeyColumn(query.getKey(),query.getSliceEnd());
        List<Entry> result = new ArrayList<Entry>(query.hasLimit()?query.getLimit():100);
        Lock lock = getLock(query.getKey(),txh);
        try {
            lock.lock();
            ConcurrentNavigableMap<KeyColumn,ByteBuffer> sub = kcv.subMap(start, end);
            int counter = 0;
            for (Map.Entry<KeyColumn,ByteBuffer> e : sub.entrySet()) {
                counter++;
                if (query.hasLimit() && counter>query.getLimit()) break;
                result.add(new Entry(e.getKey().column.duplicate(), e.getValue().duplicate()));
            }
        } finally {
            lock.unlock();
        }
        return result;
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        Lock lock = getLock(key,txh);
        try {
            lock.lock();
            ByteBuffer result = kcv.get(new KeyColumn(key,column));
            if (result!=null) return result.duplicate();
            else return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        Lock lock = getLock(key,txh);
        try {
            lock.lock();
            return kcv.containsKey(new KeyColumn(key,column));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        Lock lock = getLock(key,txh);
        try {
            lock.lock();
            if (deletions!=null && !deletions.isEmpty()) {
                for (ByteBuffer c : deletions) {
                    Preconditions.checkNotNull(c);
                    kcv.remove(new KeyColumn(key,c));
                }
            }
            if (additions!=null && !additions.isEmpty()) {
                for (Entry e : additions) {
                    Preconditions.checkNotNull(e);
                    kcv.put(new KeyColumn(key,e.getColumn()),e.getValue());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        Preconditions.checkArgument(txh.getConsistencyLevel()==ConsistencyLevel.DEFAULT);
        return new RecordIterator<ByteBuffer>() {

            private final Iterator<KeyColumn> iter = kcv.keySet().iterator();
            private ByteBuffer nextKey=nextInternal(null);

            private ByteBuffer nextInternal(ByteBuffer previous) {
                ByteBuffer next = null;
                while (next==null && iter.hasNext()) {
                    next = iter.next().key.duplicate();
                    if (previous!=null && previous.equals(next)) next=null;
                }
                return next;
            }

            @Override
            public boolean hasNext() throws StorageException {
                return nextKey!=null;
            }

            @Override
            public ByteBuffer next() throws StorageException {
                if (!hasNext()) throw new NoSuchElementException();
                ByteBuffer result = nextKey;
                nextKey=nextInternal(result);
                return result;
            }

            @Override
            public void close() throws StorageException {

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

    private static final class KeyColumn implements Comparable<KeyColumn> {

        private final ByteBuffer key;
        private final ByteBuffer column;

        KeyColumn(final ByteBuffer key, final ByteBuffer column) {
            Preconditions.checkNotNull(key);
            Preconditions.checkNotNull(column);
            this.key=key;
            this.column=column;
        }

        @Override
        public boolean equals(Object oth) {
            if (this==oth) return true;
            else if (oth==null) return false;
            else if (!getClass().isInstance(oth)) return false;
            KeyColumn o = (KeyColumn)oth;
            return key.equals(o.key) && column.equals(o.column);
        }

        @Override
        public int hashCode() {
            return key.hashCode()*9743+column.hashCode();
        }

        @Override
        public int compareTo(KeyColumn keyColumn) {
            int comp = ByteBufferUtil.compare(key.duplicate(),keyColumn.key.duplicate());
            if (comp!=0) return comp;
            else return ByteBufferUtil.compare(column.duplicate(),keyColumn.column.duplicate());
        }
    }

    private Lock getLock(ByteBuffer key, StoreTransaction txh) {
        if (txh.getConsistencyLevel()==ConsistencyLevel.KEY_CONSISTENT) {
            ByteBuffer dupKey = key.duplicate();
            if (!keyLocks.contains(dupKey)) {
                keyLocks.putIfAbsent(dupKey,new ReentrantLock());
            }
            return keyLocks.get(dupKey);
        } else return NOLOCK;
    }

    private static final Lock NOLOCK = new Lock() {

        @Override
        public void lock() {
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {

        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    };

}
