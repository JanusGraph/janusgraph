package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Wraps a {@link OrderedKeyValueStore} and exposes it as a {@link KeyColumnValueStore}.
 * <p/>
 * An optional key length parameter can be specified if it is known and guaranteed that all keys
 * passed into and read through the {@link KeyColumnValueStore} have that length. If this length is
 * static, specifying that length will make the representation of a {@link KeyColumnValueStore} in a {@link OrderedKeyValueStore}
 * more concise and more performant.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class OrderedKeyValueStoreAdapter extends BaseKeyColumnValueAdapter {

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
        super(store);
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
    public KeyIterator getKeys(final KeyRangeQuery keyQuery, final StoreTransaction txh) throws StorageException {
        return new KeyIteratorImpl(keyQuery, store.getSlice(concatenatePrefix(keyQuery.getKeyStart(), keyQuery.getSliceStart()),
                concatenatePrefix(keyQuery.getKeyEnd(), keyQuery.getSliceEnd()),
                new KeyRangeSliceSelector(keyQuery),
                txh));
    }


    @Override
    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException("This store has ordered keys, use getKeys(KeyRangeQuery, StoreTransaction) instead");
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        store.acquireLock(concatenate(key, column), expectedValue, txh);
    }

    private List<Entry> convert(RecordIterator<KeyValueEntry> entries) throws StorageException {
        List<Entry> newentries = new ArrayList<Entry>(entries.hasNext() ? 20 : 0);
        while (entries.hasNext()) {
            KeyValueEntry entry = entries.next();
            newentries.add(getEntry(entry));
        }
        try {
            entries.close();
        } catch (IOException e) {
            /*
             * IOException could be permanent or temporary. Choosing temporary
             * allows useful retries of transient failures but also allows
             * futile retries of permanent failures.
             */
            throw new TemporaryStorageException(e);
        }
        return newentries;
    }

    private Entry getEntry(KeyValueEntry entry) {
        return new StaticBufferEntry(getColumn(entry.getKey()), entry.getValue());
    }

    private boolean hasFixedKeyLength() {
        return keyLength > 0;
    }

    private int getLength(StaticBuffer key) {
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

    private StaticBuffer concatenatePrefix(StaticBuffer front, StaticBuffer end) {
        return concatenate(front, end, false);
    }

    private StaticBuffer concatenate(StaticBuffer front, StaticBuffer end, final boolean appendLength) {
        final boolean addKeyLength = !hasFixedKeyLength() && appendLength;
        int length = getLength(front);

        byte[] result = new byte[length + end.length() + (addKeyLength ? variableKeyLengthSize : 0)];
        int position = 0;
        for (int i = 0; i < front.length(); i++) result[position++] = front.getByte(i);
        for (int i = 0; i < end.length(); i++) result[position++] = end.getByte(i);

        if (addKeyLength) {
            result[position++] = (byte) (length >>> 8);
            result[position++] = (byte) length;
        }
        return new StaticArrayBuffer(result);
    }

    private StaticBuffer getColumn(StaticBuffer concat) {
        int offset = getKeyLength(concat);
        int length = concat.length() - offset;
        if (!hasFixedKeyLength()) { //variable key length => remove length at end
            length -= variableKeyLengthSize;
        }
        return concat.subrange(offset, length);
    }

    private int getKeyLength(StaticBuffer concat) {
        int length = keyLength;
        if (!hasFixedKeyLength()) { //variable key length
            length = concat.getShort(concat.length() - variableKeyLengthSize);
        }
        return length;
    }

    private StaticBuffer getKey(StaticBuffer concat) {
        return concat.subrange(0, getKeyLength(concat));
    }

    private boolean equalKey(StaticBuffer concat, StaticBuffer key) {
        int keylen = getKeyLength(concat);
        for (int i = 0; i < keylen; i++) if (concat.getByte(i) != key.getByte(i)) return false;
        return true;
    }

    private boolean columnInRange(StaticBuffer concat, StaticBuffer columnStart, StaticBuffer columnEnd) {
        StaticBuffer column = getColumn(concat);
        return column.compareTo(columnStart) >= 0 && column.compareTo(columnEnd) < 0;
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

    private class KeyRangeSliceSelector implements KeySelector {
        private final KeyRangeQuery query;

        public KeyRangeSliceSelector(KeyRangeQuery query) {
            this.query = query;
        }

        @Override
        public boolean include(StaticBuffer keycolumn) {
            StaticBuffer key = getKey(keycolumn);
            return !(key.compareTo(query.getKeyStart()) < 0 || key.compareTo(query.getKeyEnd()) >= 0)
                    && columnInRange(keycolumn, query.getSliceStart(), query.getSliceEnd());

        }

        @Override
        public boolean reachedLimit() {
            return false;
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

        private int count = 0;

        @Override
        public boolean include(StaticBuffer keyAndColumn) {
            Preconditions.checkArgument(count < limit);

            if (equalKey(keyAndColumn, key)) {
                count++;
                return true;
            }

            return false;
        }

        @Override
        public boolean reachedLimit() {
            return count >= limit;
        }

    }

    private class KeyIteratorImpl implements KeyIterator {

        private final KeyRangeQuery query;
        private final RecordIterator<KeyValueEntry> iter;

        private StaticBuffer currentKey = null;
        private EntryIterator currentIter = null;
        private boolean currentKeyReturned = true;
        private KeyValueEntry current;

        private KeyIteratorImpl(KeyRangeQuery query, RecordIterator<KeyValueEntry> iter) {
            this.query = query;
            this.iter = iter;
        }

        private StaticBuffer nextKey() throws StorageException {
            while (iter.hasNext()) {
                current = iter.next();
                StaticBuffer key = getKey(current.getKey());
                if (currentKey == null || !key.equals(currentKey)) {
                    return key;
                }
            }
            return null;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            Preconditions.checkNotNull(currentIter);
            return currentIter;
        }

        @Override
        public boolean hasNext() {
            if (currentKeyReturned) {
                try {
                    currentKey = nextKey();
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                }
                currentKeyReturned = false;

                if (currentIter != null)
                    currentIter.close();

                currentIter = new EntryIterator();
            }

            return currentKey != null;
        }

        @Override
        public StaticBuffer next() {
            if (!hasNext())
                throw new NoSuchElementException();

            currentKeyReturned = true;
            return currentKey;
        }

        @Override
        public void close() throws IOException {
            iter.close();
        }

        private class EntryIterator implements RecordIterator<Entry>, Closeable {
            private boolean open = true;
            private int count = 0;

            @Override
            public boolean hasNext() {
                Preconditions.checkState(open);

                if (current == null || count >= query.getLimit())
                    return false;

                // We need to check what is "current" right now and notify parent iterator
                // about change of main key otherwise we would be missing portion of the results
                StaticBuffer nextKey = getKey(current.getKey());
                if (!nextKey.equals(currentKey)) {
                    currentKey = nextKey;
                    currentKeyReturned = false;
                    return false;
                }

                return true;
            }

            @Override
            public Entry next() {
                Preconditions.checkState(open);

                if (!hasNext())
                    throw new NoSuchElementException();

                Entry kve = getEntry(current);
                current = iter.hasNext() ? iter.next() : null;
                count++;

                return kve;
            }

            @Override
            public void close() {
                open = false;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
