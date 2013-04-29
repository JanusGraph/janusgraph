package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Utility methods for interacting with {@link KeyValueStore}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class KVUtil {

    public static final List<KeyValueEntry> getSlice(OrderedKeyValueStore store, ByteBuffer keyStart, ByteBuffer keyEnd, StoreTransaction txh) throws StorageException {
        return store.getSlice(keyStart,keyEnd,KeySelector.SelectAll,txh);
    }

    public static final List<KeyValueEntry> getSlice(OrderedKeyValueStore store, ByteBuffer keyStart, ByteBuffer keyEnd, int limit, StoreTransaction txh) throws StorageException {
        return store.getSlice(keyStart,keyEnd,new LimitedSelector(limit),txh);
    }


}
