package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
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
            log.warn("GET query returned more than 1 result: store {} | key {} | column {}", new Object[]{store.getName(),
                    key, column});

        if (result.isEmpty()) return null;
        else return result.get(0).getValueAs(StaticBuffer.STATIC_FACTORY);
    }

    /**
     * If the store supports unordered scans, then call {#link
     * {@link KeyColumnValueStore#getKeys(SliceQuery, StoreTransaction)}. The
     * {@code SliceQuery} bounds are a binary all-zeros with an all-ones buffer.
     * The limit is 1.
     * <p>
     * If the store supports ordered scans, then call {#link
     * {@link KeyColumnValueStore#getKeys(KeyRangeQuery, StoreTransaction)}. The
     * key and columns slice bounds are the same as those described above. The
     * column limit is 1.
     *
     * @param store the store to query
     * @param features the store's features
     * @param keyLength length of the zero/one buffers that form the key limits
     * @param sliceLength length of the zero/one buffers that form the col limits
     * @param txh transaction to use with getKeys
     * @return keys returned by the store.getKeys call
     * @throws com.thinkaurelius.titan.diskstorage.BackendException unexpected failure
     */
    public static KeyIterator getKeys(KeyColumnValueStore store, StoreFeatures features, int keyLength, int sliceLength, StoreTransaction txh) throws BackendException {
        SliceQuery slice = new SliceQuery(BufferUtil.zeroBuffer(sliceLength), BufferUtil.oneBuffer(sliceLength)).setLimit(1);
        if (features.hasUnorderedScan()) {
            return store.getKeys(slice, txh);
        } else if (features.hasOrderedScan()) {
            return store.getKeys(new KeyRangeQuery(BufferUtil.zeroBuffer(keyLength), BufferUtil.oneBuffer(keyLength), slice), txh);
        } else throw new UnsupportedOperationException("Scan not supported by this store");
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

    private static final StaticBuffer START = BufferUtil.zeroBuffer(8), END = BufferUtil.oneBuffer(32);

    public static boolean containsKey(KeyColumnValueStore store, StaticBuffer key, StoreTransaction txh) throws BackendException {
        return containsKey(store,key,32,txh);
    }

    public static boolean containsKey(KeyColumnValueStore store, StaticBuffer key, int maxColumnLength, StoreTransaction txh) throws BackendException {
        StaticBuffer start = START, end = END;
        if (maxColumnLength>32) {
            end = BufferUtil.oneBuffer(maxColumnLength);
        }
        return !store.getSlice(new KeySliceQuery(key, START, END).setLimit(1),txh).isEmpty();
    }

    public static boolean matches(SliceQuery query, StaticBuffer column) {
        return query.getSliceStart().compareTo(column)<=0 && query.getSliceEnd().compareTo(column)>0;
    }

    public static boolean matches(KeyRangeQuery query, StaticBuffer key, StaticBuffer column) {
        return matches(query,column) && query.getKeyStart().compareTo(key)<=0 && query.getKeyEnd().compareTo(key)>0;

    }


    public static Map<StaticBuffer,EntryList> emptyResults(List<StaticBuffer> keys) {
        Map<StaticBuffer,EntryList> result = new HashMap<StaticBuffer, EntryList>(keys.size());
        for (StaticBuffer key : keys) {
            result.put(key,EntryList.EMPTY_LIST);
        }
        return result;
    }
}
