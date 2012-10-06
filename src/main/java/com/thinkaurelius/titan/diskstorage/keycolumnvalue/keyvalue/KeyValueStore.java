package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

public interface KeyValueStore {

    /**
     * Returns an iterator over all keys in this store. The keys may be
     * ordered but not necessarily.
     *
     * @return An iterator over all keys in this store.
     */
    public RecordIterator<ByteBuffer> getKeys(StoreTransactionHandle txh) throws StorageException;

	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, KeySelector selector, StoreTransactionHandle txh) throws StorageException;
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, StoreTransactionHandle txh) throws StorageException;
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, StoreTransactionHandle txh) throws StorageException;

    public void insert(ByteBuffer key, ByteBuffer value, StoreTransactionHandle txh) throws StorageException;

    public void delete(ByteBuffer key, StoreTransactionHandle txh) throws StorageException;

    public ByteBuffer get(ByteBuffer key, StoreTransactionHandle txh) throws StorageException;

    public boolean containsKey(ByteBuffer key, StoreTransactionHandle txh) throws StorageException;

    public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, StoreTransactionHandle txh) throws StorageException;
    
    public String getName();

    public void close() throws StorageException;

}
