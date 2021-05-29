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

package org.janusgraph.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.graphdb.query.BaseQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Wraps a {@link OrderedKeyValueStore} and exposes it as a {@link KeyColumnValueStore}.
 * <p>
 * An optional key length parameter can be specified if it is known and guaranteed that all keys
 * passed into and read through the {@link KeyColumnValueStore} have that length. If this length is
 * static, specifying that length will make the representation of a {@link KeyColumnValueStore} in a {@link OrderedKeyValueStore}
 * more concise and more efficient.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class OrderedKeyValueStoreAdapter extends BaseKeyColumnValueAdapter {

    private static final Logger log = LoggerFactory.getLogger(OrderedKeyValueStoreAdapter.class);

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
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        return convert(store.getSlice(convertQuery(query), txh));
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        final List<KVQuery> queries = new ArrayList<>(keys.size());
        for (StaticBuffer key : keys) {
            queries.add(convertQuery(new KeySliceQuery(key, query)));
        }
        final Map<KVQuery,RecordIterator<KeyValueEntry>> results = store.getSlices(queries,txh);
        final Map<StaticBuffer,EntryList> convertedResults = new HashMap<>(keys.size());
        assert queries.size()==keys.size();
        for (int i = 0; i < queries.size(); i++) {
            convertedResults.put(keys.get(i),convert(results.get(queries.get(i))));
        }
        return convertedResults;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        if (!deletions.isEmpty()) {
            for (StaticBuffer deletion : deletions) {
                StaticBuffer del = concatenate(key, deletion.as(StaticBuffer.STATIC_FACTORY));
                store.delete(del, txh);
            }

        }
        if (!additions.isEmpty()) {
            for (Entry entry : additions) {
                StaticBuffer newKey = concatenate(key, entry.getColumnAs(StaticBuffer.STATIC_FACTORY));
                store.insert(newKey, entry.getValueAs(StaticBuffer.STATIC_FACTORY), txh, (Integer) entry.getMetaData().get(EntryMetaData.TTL));
            }
        }
    }


    @Override
    public KeyIterator getKeys(final KeyRangeQuery keyQuery, final StoreTransaction txh) throws BackendException {
        final KVQuery query = new KVQuery(
                concatenatePrefix(adjustToLength(keyQuery.getKeyStart()), keyQuery.getSliceStart()),
                concatenatePrefix(adjustToLength(keyQuery.getKeyEnd()), keyQuery.getSliceEnd()), keycolumn -> {
                    final StaticBuffer key = getKey(keycolumn);
                    return !(key.compareTo(keyQuery.getKeyStart()) < 0 || key.compareTo(keyQuery.getKeyEnd()) >= 0)
                            && columnInRange(keycolumn, keyQuery.getSliceStart(), keyQuery.getSliceEnd());
                },
                BaseQuery.NO_LIMIT); //limit will be introduced in iterator

        return new KeyIteratorImpl(keyQuery,store.getSlice(query,txh));
    }

    private StaticBuffer adjustToLength(StaticBuffer key) {
        if (hasFixedKeyLength() && key.length()!=keyLength) {
            if (key.length()>keyLength) {
                return key.subrange(0,keyLength);
            } else { //Append 0s
                return BufferUtil.padBuffer(key,keyLength);
            }
        }
        return key;
    }



    @Override
    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("This store has ordered keys, use getKeys(KeyRangeQuery, StoreTransaction) instead");
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("This store has ordered keys, use getKeys(KeyRangeQuery, StoreTransaction) instead");
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
                            StoreTransaction txh) throws BackendException {
        store.acquireLock(concatenate(key, column), expectedValue, txh);
    }

    private EntryList convert(RecordIterator<KeyValueEntry> entries) throws BackendException {
        try {
            return StaticArrayEntryList.ofStaticBuffer(entries,kvEntryGetter);
        } finally {
            try {
                entries.close();
            } catch (IOException e) {
            /*
             * IOException could be permanent or temporary. Choosing temporary
             * allows useful retries of transient failures but also allows
             * futile retries of permanent failures.
             */
                throw new TemporaryBackendException(e);
            }
        }
    }

    private final StaticArrayEntry.GetColVal<KeyValueEntry,StaticBuffer> kvEntryGetter = new StaticArrayEntry.GetColVal<KeyValueEntry,StaticBuffer>() {

        @Override
        public StaticBuffer getColumn(KeyValueEntry element) {
            return getColumnFromKey(element.getKey());
        }

        @Override
        public StaticBuffer getValue(KeyValueEntry element) {
            return element.getValue();
        }

        @Override
        public EntryMetaData[] getMetaSchema(KeyValueEntry element) {
            return StaticArrayEntry.EMPTY_SCHEMA;
        }

        @Override
        public Object getMetaData(KeyValueEntry element, EntryMetaData meta) {
            throw new UnsupportedOperationException("Unsupported meta data: " + meta);
        }
    };

    private Entry getEntry(KeyValueEntry entry) {
        return StaticArrayEntry.ofStaticBuffer(entry,kvEntryGetter);
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

    final KeyValueEntry concatenate(StaticBuffer front, Entry entry) {
        return new KeyValueEntry(concatenate(front, entry.getColumnAs(StaticBuffer.STATIC_FACTORY)),
                entry.getValueAs(StaticBuffer.STATIC_FACTORY));
    }

    final KVQuery convertQuery(final KeySliceQuery query) {
        Predicate<StaticBuffer> filter = Predicates.alwaysTrue();
        if (!hasFixedKeyLength()) {
            filter = keyAndColumn -> equalKey(keyAndColumn, query.getKey());
        }
        return new KVQuery(
                concatenatePrefix(query.getKey(), query.getSliceStart()),
                concatenatePrefix(query.getKey(), query.getSliceEnd()),
                filter,query.getLimit());
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
            result[position] = (byte) length;
        }
        return StaticArrayBuffer.of(result);
    }

    private StaticBuffer getColumnFromKey(StaticBuffer concat) {
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
        int keyLength = getKeyLength(concat);
        for (int i = 0; i < keyLength; i++) if (concat.getByte(i) != key.getByte(i)) return false;
        return true;
    }

    private boolean columnInRange(StaticBuffer concat, StaticBuffer columnStart, StaticBuffer columnEnd) {
        StaticBuffer column = getColumnFromKey(concat);
        return column.compareTo(columnStart) >= 0 && column.compareTo(columnEnd) < 0;
    }

    private class KeyIteratorImpl implements KeyIterator {

        private final KeyRangeQuery query;
        private final RecordIterator<KeyValueEntry> iterator;

        private StaticBuffer currentKey = null;
        private EntryIterator currentIterator = null;
        private boolean currentKeyReturned = true;
        private KeyValueEntry current;

        private KeyIteratorImpl(KeyRangeQuery query, RecordIterator<KeyValueEntry> iterator) {
            this.query = query;
            this.iterator = iterator;
        }

        private StaticBuffer nextKey() {
            while (iterator.hasNext()) {
                current = iterator.next();
                StaticBuffer key = getKey(current.getKey());
                if (!key.equals(currentKey)) {
                    return key;
                }
            }
            return null;
        }

        private void resetKey(StaticBuffer nextKey) {
            currentKey = nextKey;
            currentKeyReturned = false;
            if (currentIterator != null)
                currentIterator.close();
            currentIterator = new EntryIterator();
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            Preconditions.checkNotNull(currentIterator);
            return currentIterator;
        }

        @Override
        public boolean hasNext() {
            if (currentKeyReturned) {
                resetKey(nextKey());
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
            iterator.close();
        }

        private class EntryIterator implements RecordIterator<Entry>, Closeable {
            private boolean open = true;
            private int count = 0;

            @Override
            public boolean hasNext() {
                Preconditions.checkState(open);

                if (current == null)
                    return false;

                // We need to check what is "current" right now and notify parent iterator
                // about change of main key otherwise we would be missing portion of the results
                StaticBuffer nextKey = getKey(current.getKey());
                if (!nextKey.equals(currentKey)) {
                    resetKey(nextKey);
                    return false;
                }

                return count < query.getLimit();
            }

            @Override
            public Entry next() {
                Preconditions.checkState(open);

                if (!hasNext())
                    throw new NoSuchElementException();

                Entry kve = getEntry(current);
                current = iterator.hasNext() ? iterator.next() : null;
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
