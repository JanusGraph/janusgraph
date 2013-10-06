package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
    public static StaticBuffer get(KeyColumnValueStore store, StaticBuffer key, StaticBuffer column, StoreTransaction txh) throws StorageException {
        KeySliceQuery query = new KeySliceQuery(key, column, ByteBufferUtil.nextBiggerBuffer(column)).setLimit(2);
        List<Entry> result = store.getSlice(query, txh);
        if (result.size() > 1)
            log.warn("GET query returned more than 1 result: store {} | key {} | column {}", new Object[]{store.getName(),
                    key, column});

        if (result.isEmpty()) return null;
        else return result.get(0).getValue();
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
     * @throws StorageException unexpected failure
     */
    public static RecordIterator<StaticBuffer> getKeys(KeyColumnValueStore store, StoreFeatures features, int keyLength, int sliceLength, StoreTransaction txh) throws StorageException {
        SliceQuery slice = new SliceQuery(ByteBufferUtil.zeroBuffer(sliceLength), ByteBufferUtil.oneBuffer(sliceLength)).setLimit(1);
        if (features.supportsUnorderedScan()) {
            return store.getKeys(slice, txh);
        } else if (features.supportsOrderedScan()) {
            return store.getKeys(new KeyRangeQuery(ByteBufferUtil.zeroBuffer(keyLength), ByteBufferUtil.oneBuffer(keyLength), slice), txh);
        } else throw new UnsupportedOperationException("Scan not supported by this store");
    }

    public static boolean containsKey(KeyColumnValueStore store, StaticBuffer key, int sliceLength, StoreTransaction txh) throws StorageException {
        SliceQuery slice = new SliceQuery(ByteBufferUtil.zeroBuffer(sliceLength), ByteBufferUtil.oneBuffer(sliceLength)).setLimit(1);
        return !store.getSlice(new KeySliceQuery(key, slice), txh).isEmpty();
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
    public static boolean containsKeyColumn(KeyColumnValueStore store, StaticBuffer key, StaticBuffer column, StoreTransaction txh) throws StorageException {
        return get(store, key, column, txh) != null;
    }

    public static boolean matches(SliceQuery query, StaticBuffer column) {
        return query.getSliceStart().compareTo(column)<=0 && query.getSliceEnd().compareTo(column)>0;
    }

    public static boolean matches(KeyRangeQuery query, StaticBuffer key, StaticBuffer column) {
        return matches(query,column) && query.getKeyStart().compareTo(key)<=0 && query.getKeyEnd().compareTo(key)>0;

    }


}
