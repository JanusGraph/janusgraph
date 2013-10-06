package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BackendCompression;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheStoreAdapter extends BaseKeyColumnValueAdapter {

    private final Logger log = LoggerFactory.getLogger(CacheStoreAdapter.class);

    private static final int MAX_BYTE_LEN = 1024 * 1024 * 512; //512 MB

    private static final int COLUMN_LEN_BYTES = 2; //maximum short length
    private static final int VALUE_LEN_BYTES = 4; //maximum integer length

    private final CacheStore store;
    private final BackendCompression compression = BackendCompression.NO_COMPRESSION;
    private final int maxMutationRetries = 10;
    private final int mutationRetryWaitTimeMS = 50;

    public CacheStoreAdapter(CacheStore store) {
        super(store);
        this.store = store;
    }

    private final StaticBuffer decompress(StaticBuffer value) {
        if (value == null) return null;
        else return compression.decompress(value);
    }

    private final StaticBuffer compress(StaticBuffer value) {
        if (value == null) return null;
        else return compression.compress(value);
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        StaticBuffer value = decompress(store.get(query.getKey(), txh));
        return new CacheEntryIterator(value, query).toList(query.getLimit());
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(key, txh);
    }

    @Override
    public void mutate(final StaticBuffer key, final List<Entry> additions, final List<StaticBuffer> deletions, final StoreTransaction txh) throws StorageException {
        if (additions.isEmpty() && deletions.isEmpty()) return;

        if (additions.size() > 1) Collections.sort(additions);
        int additionalLength = 0;
        for (Entry e : additions) {
            additionalLength += COLUMN_LEN_BYTES + VALUE_LEN_BYTES;
            additionalLength += e.getColumn().length();
            additionalLength += e.getValue().length();
        }
        if (deletions.size() > 1) Collections.sort(deletions);
        final int addLength = additionalLength;

        BackendOperation.execute(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                StaticBuffer oldValue = decompress(store.get(key, txh));
                int oldLen = oldValue == null ? 0 : oldValue.length();
                int newLen = oldLen + addLength;
                Preconditions.checkArgument(newLen < MAX_BYTE_LEN, "New allocation [%s] exceeded max value length [%s] ", newLen, MAX_BYTE_LEN);
                ByteBuffer out = ByteBuffer.allocate(newLen);

                int oldindex = 0;
                int addindex = 0;
                int delindex = 0;
                while (oldindex < oldLen) {
                    int collen = fromUnsignedShort(oldValue.getShort(oldindex));
                    oldindex += COLUMN_LEN_BYTES;
                    int vallen = oldValue.getInt(oldindex);
                    oldindex += VALUE_LEN_BYTES;
                    StaticBuffer col = oldValue.subrange(oldindex, collen);
                    int cmp = -1;
                    boolean replace = false;
                    while (addindex < additions.size() && (cmp = col.compareTo(additions.get(addindex).getColumn())) >= 0) {
                        //insert before
                        insert(additions.get(addindex), out);
                        addindex++;
                        if (cmp == 0) replace = true;
                    }
                    if (delindex < deletions.size() && col.compareTo(deletions.get(delindex)) == 0) {
                        delindex++;
                    } else if (!replace) {
                        insert(col, oldValue.subrange(oldindex + collen, vallen), out);
                    }
                    //Iterate out missing deletions
                    while (delindex < deletions.size() && col.compareTo(deletions.get(delindex)) >= 0) {
                        delindex++;
                    }
                    oldindex += collen + vallen;
                }
                //Write remaining additions
                while (addindex < additions.size()) {
                    insert(additions.get(addindex), out);
                    addindex++;
                }
                out.flip();
                if (!out.hasRemaining()) {
                    store.delete(key, txh);
                } else {
                    StaticBuffer newValue = compress(new StaticByteBuffer(out));
                    store.replace(key, newValue, oldValue, txh);
                }
                return null;
            }
        }, maxMutationRetries, mutationRetryWaitTimeMS);
    }

    private static final void insert(Entry entry, ByteBuffer out) {
        insert(entry.getColumn(), entry.getValue(), out);
    }

    private static final void insert(StaticBuffer col, StaticBuffer val, ByteBuffer out) {
        out.putShort(toUnsignedShort(col.length()));
        Preconditions.checkArgument(val.length() >= 0 && val.length() <= Integer.MAX_VALUE);
        out.putInt(val.length());
        writeStaticBuffer(col, out);
        writeStaticBuffer(val, out);
    }

    private static final void writeStaticBuffer(StaticBuffer buffer, ByteBuffer out) {
        for (int i = 0; i < buffer.length(); i++) {
            out.put(buffer.getByte(i));
        }
    }

    private static final short toUnsignedShort(int value) {
//        Preconditions.checkArgument(value>=0 && value<Short.MAX_VALUE-Short.MIN_VALUE,"Value out of range: %s",value);
//        return (short)(value+Short.MIN_VALUE);
        Preconditions.checkArgument(value >= 0 && value <= Short.MAX_VALUE, "Value out of range: %s", value);
        return (short) (value);
    }

    private static final int fromUnsignedShort(short value) {
//        return ((int)value)-Short.MIN_VALUE;
        Preconditions.checkArgument(value >= 0);
        return value;
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        return new CacheKeyIterator(store.getKeys(new KVUtil.RangeKeySelector(
                query.getKeyStart(), query.getKeyEnd()), txh), query);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        return new CacheKeyIterator(store.getKeys(KeySelector.SelectAll, txh), query);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException(); //TODO: implement
    }

    private class CacheKeyIterator implements KeyIterator {

        private final SliceQuery slice;
        private final RecordIterator<KeyValueEntry> underlyingIter;
        private final Iterator<KeyValueEntry> iter;

        private KeyValueEntry entry;

        private CacheKeyIterator(RecordIterator<KeyValueEntry> iter, final SliceQuery slice) {
            this.slice = slice;
            this.underlyingIter = iter;
            this.iter = Iterators.filter(iter, new Predicate<KeyValueEntry>() {

                // Some of this logic probably shares enough commonality with
                // CacheEntryIterator and mutate() to warrant a refactoring that
                // makes all three share common pieces of KVE-handling code
                @Override
                public boolean apply(KeyValueEntry input) {
                    StaticBuffer value = input.getValue();
                    int index = 0;
                    while (index < value.length()) {
                        int collen = fromUnsignedShort(value.getShort(index));
                        index += COLUMN_LEN_BYTES;
                        int vallen = value.getInt(index);
                        index += VALUE_LEN_BYTES;
                        StaticBuffer col = value.subrange(index, collen);
                        if (col.compareTo(slice.getSliceStart()) >= 0 && col.compareTo(slice.getSliceEnd()) < 0) {
                            return true;
                        }
                        index += collen + vallen;
                    }
                    return false;
                }
                
            });
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            Preconditions.checkState(entry != null);
            return new CacheEntryIterator(decompress(entry.getValue()), slice);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public StaticBuffer next() {
            entry = iter.next();
            return entry.getKey();
        }

        @Override
        public void close() throws IOException {
            underlyingIter.close();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class CacheEntryIterator implements RecordIterator<Entry> {

        private StaticBuffer value;
        private final SliceQuery slice;

        private int index = 0;
        private boolean foundStart = false;
        private Entry nextEntry;

        private CacheEntryIterator(StaticBuffer value, SliceQuery slice) {
            this.value = value;
            this.slice = slice;

            if (value == null) this.nextEntry = null;
            else this.nextEntry = getNextEntry();
        }

        public List<Entry> toList(final int limit) throws StorageException {
            List<Entry> resultSet = new ArrayList<Entry>(Math.min(100, limit));
            while (hasNext() && resultSet.size() < limit) resultSet.add(next());
            close();
            return resultSet;
        }

        private Entry getNextEntry() {
            while (index < value.length()) {
                int collen = fromUnsignedShort(value.getShort(index));
                index += COLUMN_LEN_BYTES;
                int vallen = value.getInt(index);
                index += VALUE_LEN_BYTES;
                StaticBuffer col = value.subrange(index, collen);
                if (!foundStart) {
                    if (col.compareTo(slice.getSliceStart()) >= 0) {
                        foundStart = true;
                    } else {
                        index += collen + vallen;
                        continue;
                    }
                }
                if (foundStart && col.compareTo(slice.getSliceEnd()) >= 0) //the end
                    return null;

                StaticBuffer val = value.subrange(index + collen, vallen);
                index += collen + vallen;
                return new StaticBufferEntry(col, val);
            }
            return null;
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public Entry next() {
            if (!hasNext()) throw new NoSuchElementException();
            Entry result = nextEntry;
            nextEntry = getNextEntry();
            return result;
        }

        @Override
        public void close() {
            //release memory
            value = null;
            nextEntry = null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
