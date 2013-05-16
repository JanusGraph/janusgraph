package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.List;

/**
 * A {@link KeyValueStore} where the keys are ordered such that keys can be retrieved in order.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface OrderedKeyValueStore extends KeyValueStore {

    /**
     * Returns a list of all Key-value pairs ({@link KeyValueEntry} where the key lies between keyStart (inclusive) and keyEnd (exclusive)
     * and such that they match the specified {@link KeySelector}. If the KeySelector reaches its limit before the specified range is
     * exhausted, only the matching entries up to that point are returned.
     *
     * The operation is executed inside the context of the given transaction.
     *
     * @param keyStart
     * @param keyEnd
     * @param selector
     * @param txh
     * @return
     * @throws StorageException
     */
    public List<KeyValueEntry> getSlice(StaticBuffer keyStart, StaticBuffer keyEnd, KeySelector selector, StoreTransaction txh) throws StorageException;

    //TODO: Add for Fulgora and support for new KeyColumnValueStore methods
    //public RecordIterator<KeyValueEntry> getEntries(StaticBuffer keyStart, StaticBuffer keyEnd, StoreTransaction txh) throws StorageException;

}
