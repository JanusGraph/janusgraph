package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

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
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        ContainsSelector select = new ContainsSelector(key);
        store.getSlice(key, ByteBufferUtil.nextBiggerBuffer(key), select, txh);
        return select.contains();
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return convert(store.getSlice(concatenatePrefix(query.getKey(), query.getSliceStart()), concatenatePrefix(query.getKey(), query.getSliceEnd()),
                new KeyColumnSliceSelector(query.getKey(), query.getLimit()), txh));
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        if (!deletions.isEmpty()) {
            for (ByteBuffer column : deletions) {
                ByteBuffer del = concatenate(key, column);
                store.delete(del, txh);
            }

        }
        if (!additions.isEmpty()) {
            for (Entry entry : additions) {
                ByteBuffer newkey = concatenate(key, entry.getColumn());
                store.insert(newkey, entry.getValue(), txh);
            }
        }
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return new KeysIterator(store.getKeys(txh));
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        return store.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue,
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
            newentries.add(new CacheEntry(getColumn(entry.getKey()), entry.getValue()));
        }
        return newentries;
    }

    private Map<ByteBuffer, List<Entry>> convertKey(List<KeyValueEntry> entries) {
        if (entries == null) return null;
        Map<ByteBuffer, List<Entry>> keyentries = new HashMap<ByteBuffer, List<Entry>>((int) Math.sqrt(entries.size()));
        ByteBuffer key = null;
        List<Entry> newentries = null;
        for (KeyValueEntry entry : entries) {
            ByteBuffer currentKey = getKey(entry.getKey());
            if (key == null || !key.equals(currentKey)) {
                if (key != null) {
                    assert newentries != null;
                    keyentries.put(key, newentries);
                }
                key = currentKey;
                newentries = new ArrayList<Entry>((int) Math.sqrt(entries.size()));
            }
            newentries.add(new CacheEntry(getColumn(entry.getKey()), entry.getValue()));
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

    private final int getLength(ByteBuffer key) {
        int length = keyLength;
        if (hasFixedKeyLength()) { //fixed key length
            Preconditions.checkArgument(key.remaining() == length);
        } else { //variable key length
            length = key.remaining();
            Preconditions.checkArgument(length < maxVariableKeyLength);
        }
        return length;
    }

    final ByteBuffer concatenate(ByteBuffer front, ByteBuffer end) {
        return concatenate(front, end, true);
    }

    private final ByteBuffer concatenatePrefix(ByteBuffer front, ByteBuffer end) {
        return concatenate(front, end, false);
    }

    private final ByteBuffer concatenate(ByteBuffer front, ByteBuffer end, final boolean appendLength) {
        final boolean addKeyLength = !hasFixedKeyLength() && appendLength;
        int length = getLength(front);

        ByteBuffer result = ByteBuffer.allocate(length + end.remaining() + (addKeyLength ? variableKeyLengthSize : 0));

        front.mark();
        result.put(front);
        front.reset();
        end.mark();
        result.put(end);
        end.reset();

        if (addKeyLength) result.putShort((short) length);

        result.flip();
        front.reset();
        end.reset();
        return result;
    }

    private final ByteBuffer getColumn(ByteBuffer concat) {
        concat.position(getKeyLength(concat));
        ByteBuffer column = concat.slice();
        if (!hasFixedKeyLength()) { //variable key length => remove length at end
            column.limit(column.limit() - variableKeyLengthSize);
        }
        return column;
    }

    private final int getKeyLength(ByteBuffer concat) {
        int length = keyLength;
        if (!hasFixedKeyLength()) { //variable key length
            length = concat.getShort(concat.limit() - variableKeyLengthSize);
        }
        return length;
    }

    private final ByteBuffer getKey(ByteBuffer concat) {
        ByteBuffer key = concat.duplicate();
        key.limit(key.position() + getKeyLength(concat));
        return key;
    }

    private final boolean equalKey(ByteBuffer concat, ByteBuffer key) {
        int oldlimit = concat.limit();
        concat.limit(concat.position() + getKeyLength(concat));
        boolean equals = key.equals(concat);
        concat.limit(oldlimit);
        return equals;
    }

    private final boolean columnInRange(ByteBuffer concat, ByteBuffer columnStart,
                                        ByteBuffer columnEnd, boolean startInc, boolean endInc) {
        int oldposition = concat.position(), oldlimit = concat.limit();
        concat.position(getKeyLength(concat));
        if (!hasFixedKeyLength()) concat.limit(concat.limit() - variableKeyLengthSize);
        int startComp = ByteBufferUtil.compare(columnStart,concat);
        int endComp = ByteBufferUtil.compare(concat,columnEnd);
        boolean inrange = (startComp<0 || (startComp==0 && startInc)) && (endComp<0 || (endComp==0 && endInc));
        concat.position(oldposition);
        concat.limit(oldlimit);
        return inrange;
    }

    private class ContainsSelector implements KeySelector {

        private final ByteBuffer checkKey;
        private boolean contains = false;

        private ContainsSelector(ByteBuffer key) {
            checkKey = key;
        }

        public boolean contains() {
            return contains;
        }

        @Override
        public boolean include(ByteBuffer keycolumn) {
            contains = equalKey(keycolumn, checkKey);
            return false;
        }

        @Override
        public boolean reachedLimit() {
            return true;
        }

    }

    private class KeyColumnSliceSelector implements KeySelector {

        private final ByteBuffer key;
        private final int limit;

        public KeyColumnSliceSelector(ByteBuffer key, int limit) {
            Preconditions.checkArgument(limit > 0, "The count limit needs to be positive. Given: " + limit);
            this.key = key;
            this.limit = limit;
        }

        public KeyColumnSliceSelector(ByteBuffer key) {
            this(key, Integer.MAX_VALUE);
        }

        private int count = 0;

        @Override
        public boolean include(ByteBuffer keycolumn) {
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

    private class KeysIterator implements RecordIterator<ByteBuffer> {

        final RecordIterator<ByteBuffer> iterator;
        ByteBuffer nextKey;

        private KeysIterator(RecordIterator<ByteBuffer> iterator) throws StorageException {
            this.iterator = iterator;
            this.nextKey = null;
            getNextKey();
        }

        private void getNextKey() throws StorageException {
            boolean foundNextKey = false;
            while (!foundNextKey && iterator.hasNext()) {
                ByteBuffer keycolumn = iterator.next();
                if (nextKey == null || !equalKey(keycolumn, nextKey)) {
                    foundNextKey = true;
                    nextKey = getKey(keycolumn);
                }
            }
            if (!foundNextKey) nextKey = null;
        }

        @Override
        public boolean hasNext() throws StorageException {
            return nextKey != null;
        }

        @Override
        public ByteBuffer next() throws StorageException {
            if (nextKey == null) throw new NoSuchElementException();
            ByteBuffer returnKey = nextKey;
            getNextKey();
            return returnKey;
        }

        @Override
        public void close() throws StorageException {
            iterator.close();
        }
    }

}
