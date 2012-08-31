package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface ScanKeyValueStore extends OrderedKeyValueStore {

    /**
     * Returns an iterator over all keys in this store. The keys may be
     * ordered but not necessarily.
     *
     * @return An iterator over all keys in this store.
     */
    public RecordIterator<ByteBuffer> getKeys(TransactionHandle txh) throws StorageException;

}
