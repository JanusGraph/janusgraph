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

package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains static utility methods for operating on {@link KeyColumnValueStore}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KCVSUtil {

    private static final Logger log = LoggerFactory.getLogger(KeyColumnValueStore.class);

    private static final int MAX_COLUMN_LENGTH = 32;
    private static final StaticBuffer START = BufferUtil.zeroBuffer(1);
    private static final StaticBuffer END = BufferUtil.oneBuffer(MAX_COLUMN_LENGTH);

    /**
     * Retrieves the value for the specified column and key under the given transaction
     * from the store if such exists, otherwise returns NULL
     *
     * @param store  Store
     * @param key    Key
     * @param column Column
     * @param txh    Transaction
     * @return Value for key and column or NULL if such does not exist
     */
    public static StaticBuffer get(KeyColumnValueStore store, StaticBuffer key, StaticBuffer column, StoreTransaction txh) throws BackendException {
        KeySliceQuery query = new KeySliceQuery(key, column, BufferUtil.nextBiggerBuffer(column)).setLimit(2);
        List<Entry> result = store.getSlice(query, txh);
        if (result.size() > 1)
            log.warn("GET query returned more than 1 result: store {} | key {} | column {}", store.getName(),
                key, column);

        if (result.isEmpty()) return null;
        else return result.get(0).getValueAs(StaticBuffer.STATIC_FACTORY);
    }

    public static KeyIterator getKeys(KeyColumnValueStore store, StoreFeatures features, int keyLength, int sliceLength, StoreTransaction txh) throws BackendException {
        return getKeys(store,new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(sliceLength)).setLimit(1),
                features,keyLength,txh);
    }

    public static KeyIterator getKeys(KeyColumnValueStore store, SliceQuery slice, StoreFeatures features, int keyLength, StoreTransaction txh) throws BackendException {
        if (features.hasUnorderedScan()) {
            return store.getKeys(slice, txh);
        } else if (features.hasOrderedScan()) {
            return store.getKeys(new KeyRangeQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(keyLength), slice), txh);
        } else throw new UnsupportedOperationException("Provided stores does not support scan operations: " + store);
    }

    /**
     * Returns true if the specified key-column pair exists in the store.
     *
     * @param store  Store
     * @param key    Key
     * @param column Column
     * @param txh    Transaction
     * @return TRUE, if key has at least one column-value pair, else FALSE
     */
    public static boolean containsKeyColumn(KeyColumnValueStore store, StaticBuffer key, StaticBuffer column, StoreTransaction txh) throws BackendException {
        return get(store, key, column, txh) != null;
    }

    public static boolean containsKey(KeyColumnValueStore store, StaticBuffer key, StoreTransaction txh) throws BackendException {
        return containsKey(store, key, MAX_COLUMN_LENGTH, txh);
    }

    public static boolean containsKey(KeyColumnValueStore store, StaticBuffer key, int maxColumnLength, StoreTransaction txh) throws BackendException {
        final StaticBuffer end = maxColumnLength > MAX_COLUMN_LENGTH ? BufferUtil.oneBuffer(maxColumnLength) : END;
        return !store.getSlice(new KeySliceQuery(key, START, end).setLimit(1),txh).isEmpty();
    }

    public static boolean matches(SliceQuery query, StaticBuffer column) {
        return query.getSliceStart().compareTo(column)<=0 && query.getSliceEnd().compareTo(column)>0;
    }

    public static boolean matches(KeyRangeQuery query, StaticBuffer key, StaticBuffer column) {
        return matches(query,column) && query.getKeyStart().compareTo(key)<=0 && query.getKeyEnd().compareTo(key)>0;

    }


    public static Map<StaticBuffer,EntryList> emptyResults(List<StaticBuffer> keys) {
        final Map<StaticBuffer,EntryList> result = new HashMap<>(keys.size());
        for (StaticBuffer key : keys) {
            result.put(key,EntryList.EMPTY_LIST);
        }
        return result;
    }
}
