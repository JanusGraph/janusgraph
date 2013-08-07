package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import javax.annotation.Nullable;

/**
 * Wraps a {@link OrderedKeyValueStore} and exposes it as a {@link KeyColumnValueStore}.
 *
 * An optional key length parameter can be specified if it is known and guaranteed that all keys
 * passed into and read through the {@link KeyColumnValueStore} have that length. If this length is
 * static, specifying that length will make the representation of a {@link KeyColumnValueStore} in a {@link OrderedKeyValueStore}
 * more concise and more performant.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class OrderedKeyValueStoreAdapter implements KeyColumnValueStore {

    private final Logger log = LoggerFactory.getLogger(OrderedKeyValueStoreAdapter.class);

    public static final int variableKeyLength = 0;

    public static final int maxVariableKeyLength = Short.MAX_VALUE;
    public static final int variableKeyLengthSize = 2;

    private final OrderedKeyValueStore store;
    private final int keyLength;

    public OrderedKeyValueStoreAdapter(OrderedKeyValueStore store) {
        this(store, variableKeyLength);
    }

    public OrderedKeyValueStoreAdapter(OrderedKeyValueStore store, int keyLength) {
        Preconditions.checkNotNull(store);
        Preconditions.checkArgument(keyLength >= 0);
        this.store = store;
        this.keyLength = keyLength;
        log.debug("Used key length {} for database {}", keyLength, store.getName());
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        ContainsSelector select = new ContainsSelector(key);
        store.getSlice(key, ByteBufferUtil.nextBiggerBuffer(key), select, txh);
        return select.contains();
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return convert(store.getSlice(concatenatePrefix(query.getKey(), query.getSliceStart()),
                                      concatenatePrefix(query.getKey(), query.getSliceEnd()),
                                      new KeyColumnSliceSelector(query.getKey(), query.getLimit()), txh));
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
        if (!deletions.isEmpty()) {
            for (StaticBuffer column : deletions) {
                StaticBuffer del = concatenate(key, column);
                store.delete(del, txh);
            }

        }
        if (!additions.isEmpty()) {
            for (Entry entry : additions) {
                StaticBuffer newkey = concatenate(key, entry.getColumn());
                store.insert(newkey, entry.getValue(), txh);
            }
        }
    }

    @Override
    public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return new KeysIterator(store.getKeys(txh));
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyQuery, StoreTransaction txh) throws StorageException {
        return new KeysIterator(store.getKeys(txh), keyQuery, txh);
    }

    @Override
    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws StorageException {
        return new KeysIterator(store.getKeys(txh), columnQuery, txh);
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        return store.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        store.acquireLock(concatenate(key, column), expectedValue, txh);
    }


    @Override
    public void close() throws StorageException {
        store.close();
    }


    private List<Entry> convert(List<KeyValueEntry> entries) {
        if (entries == null) return null;
        List<Entry> newentries = new ArrayList<Entry>(entries.size());
        for (KeyValueEntry entry : entries) {
            newentries.add(new StaticBufferEntry(getColumn(entry.getKey()), entry.getValue()));
        }
        return newentries;
    }

    private Map<StaticBuffer, List<Entry>> convertKey(List<KeyValueEntry> entries) {
        if (entries == null) return null;
        Map<StaticBuffer, List<Entry>> keyentries = new HashMap<StaticBuffer, List<Entry>>((int) Math.sqrt(entries.size()));
        StaticBuffer key = null;
        List<Entry> newentries = null;
        for (KeyValueEntry entry : entries) {
            StaticBuffer currentKey = getKey(entry.getKey());
            if (key == null || !key.equals(currentKey)) {
                if (key != null) {
                    assert newentries != null;
                    keyentries.put(key, newentries);
                }
                key = currentKey;
                newentries = new ArrayList<Entry>((int) Math.sqrt(entries.size()));
            }
            newentries.add(new StaticBufferEntry(getColumn(entry.getKey()), entry.getValue()));
        }
        if (key != null) {
            assert newentries != null;
            keyentries.put(key, newentries);
        }
        return keyentries;
    }

    private final int getKeyLength() {
        return keyLength;
    }

    private final boolean hasFixedKeyLength() {
        return keyLength > 0;
    }

    private final int getLength(StaticBuffer key) {
        int length = keyLength;
        if (hasFixedKeyLength()) { //fixed key length
            Preconditions.checkArgument(key.length() == length);
        } else { //variable key length
            length = key.length();
            Preconditions.checkArgument(length < maxVariableKeyLength);
        }
        return length;
    }

    final StaticBuffer concatenate(StaticBuffer front, StaticBuffer end) {
        return concatenate(front, end, true);
    }

    private final StaticBuffer concatenatePrefix(StaticBuffer front, StaticBuffer end) {
        return concatenate(front, end, false);
    }

    private final StaticBuffer concatenate(StaticBuffer front, StaticBuffer end, final boolean appendLength) {
        final boolean addKeyLength = !hasFixedKeyLength() && appendLength;
        int length = getLength(front);

        byte[] result = new byte[length + end.length() + (addKeyLength ? variableKeyLengthSize : 0)];
        int position = 0;
        for (int i=0;i<front.length();i++) result[position++]=front.getByte(i);
        for (int i=0;i<end.length();i++) result[position++]=end.getByte(i);

        if (addKeyLength) {
            result[position++] = (byte)(length >>> 8);
            result[position++] = (byte)length;
        }
        return new StaticArrayBuffer(result);
    }

    private final StaticBuffer getColumn(StaticBuffer concat) {
        int offset = getKeyLength(concat);
        int length = concat.length()-offset;
        if (!hasFixedKeyLength()) { //variable key length => remove length at end
            length -= variableKeyLengthSize;
        }
        return concat.subrange(offset,length);
    }

    private final int getKeyLength(StaticBuffer concat) {
        int length = keyLength;
        if (!hasFixedKeyLength()) { //variable key length
            length = concat.getShort(concat.length() - variableKeyLengthSize);
        }
        return length;
    }

    private final StaticBuffer getKey(StaticBuffer concat) {
        return concat.subrange(0,getKeyLength(concat));
    }

    private final boolean equalKey(StaticBuffer concat, StaticBuffer key) {
        int keylen = getKeyLength(concat);
        for (int i=0;i<keylen;i++) if (concat.getByte(i)!=key.getByte(i)) return false;
        return true;
    }

    private final boolean columnInRange(StaticBuffer concat, StaticBuffer columnStart,
                                        StaticBuffer columnEnd, boolean startInc, boolean endInc) {
        StaticBuffer column = getColumn(concat);
        int startComp = columnStart.compareTo(column);
        int endComp = column.compareTo(columnEnd);
        return (startComp<0 || (startComp==0 && startInc)) && (endComp<0 || (endComp==0 && endInc));
    }

    private class ContainsSelector implements KeySelector {

        private final StaticBuffer checkKey;
        private boolean contains = false;

        private ContainsSelector(StaticBuffer key) {
            checkKey = key;
        }

        public boolean contains() {
            return contains;
        }

        @Override
        public boolean include(StaticBuffer keycolumn) {
            contains = equalKey(keycolumn, checkKey);
            return false;
        }

        @Override
        public boolean reachedLimit() {
            return true;
        }

    }

    private class KeyColumnSliceSelector implements KeySelector {

        private final StaticBuffer key;
        private final int limit;

        public KeyColumnSliceSelector(StaticBuffer key, int limit) {
            Preconditions.checkArgument(limit > 0, "The count limit needs to be positive. Given: " + limit);
            this.key = key;
            this.limit = limit;
        }

        public KeyColumnSliceSelector(StaticBuffer key) {
            this(key, Integer.MAX_VALUE);
        }

        private int count = 0;

        @Override
        public boolean include(StaticBuffer keycolumn) {
            Preconditions.checkArgument(count < limit);
            if (equalKey(keycolumn, key)) {
                count++;
                return true;
            } else return false;
        }

        @Override
        public boolean reachedLimit() {
            return count >= limit;
        }

    }

    protected class KeysIterator implements KeyIterator {
        final RecordIterator<StaticBuffer> iterator;
        final StaticBuffer startKey, endKey;
        final SliceQuery sliceQuery;
        final StoreTransaction txn;

        StaticBuffer nextKey, currentKey;

        public KeysIterator(RecordIterator<StaticBuffer> iterator) throws StorageException {
            this(iterator, null, null, null, null);
        }

        public KeysIterator(RecordIterator<StaticBuffer> iterator,
                             KeyRangeQuery keyRangeQuery,
                             StoreTransaction txn) throws StorageException {
            this(iterator, keyRangeQuery.getKeyStart(), keyRangeQuery.getKeyEnd(), keyRangeQuery, txn);
        }

        public KeysIterator(RecordIterator<StaticBuffer> iterator,
                             SliceQuery sliceQuery,
                             StoreTransaction txn) throws StorageException {
            this(iterator, null, null, sliceQuery, txn);
        }

        public KeysIterator(RecordIterator<StaticBuffer> iterator,
                            @Nullable StaticBuffer startKey,
                            @Nullable StaticBuffer endKey,
                            @Nullable SliceQuery sliceQuery,
                            @Nullable StoreTransaction txn) throws StorageException {
            this.iterator = iterator;
            this.nextKey = null;
            this.startKey = startKey;
            this.endKey = endKey;
            this.sliceQuery = sliceQuery;
            this.txn = txn;

            getNextKey();
            currentKey = nextKey;
        }

        private void getNextKey() throws StorageException {
            boolean foundNextKey = false;

            while (!foundNextKey && iterator.hasNext()) {
                StaticBuffer keycolumn = iterator.next();

                if (startKey != null && endKey != null && (keycolumn.compareTo(startKey) < 0 || keycolumn.compareTo(endKey) >= 0))
                    continue;

                if (nextKey == null || !equalKey(keycolumn, nextKey)) {
                    foundNextKey = true;
                    nextKey = getKey(keycolumn);
                }
            }

            if (!foundNextKey)
                nextKey = null;
        }

        @Override
        public boolean hasNext() throws StorageException {
            return nextKey != null;
        }

        @Override
        public StaticBuffer next() throws StorageException {
            if (nextKey == null)
                throw new NoSuchElementException();

            currentKey = nextKey;
            getNextKey();

            return currentKey;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            if (currentKey == null)
                throw new NoSuchElementException();

            if (sliceQuery == null || txn == null)
                throw new IllegalStateException("getEntries() could only be used when columnSlice and transaction are set.");

            try {
                return new RecordIterator<Entry>() {
                    final Iterator<Entry> entries = getSlice(new KeySliceQuery(currentKey, sliceQuery), txn).iterator();

                    @Override
                    public boolean hasNext() throws StorageException {
                        return entries.hasNext();
                    }

                    @Override
                    public Entry next() throws StorageException {
                        return entries.next();
                    }

                    @Override
                    public void close() throws StorageException {
                        iterator.close();
                    }
                };
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws StorageException {
            iterator.close();
        }
    }

}
