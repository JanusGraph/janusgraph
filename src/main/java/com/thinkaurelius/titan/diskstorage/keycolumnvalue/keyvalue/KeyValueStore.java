package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.nio.ByteBuffer;
import java.util.List;

public interface KeyValueStore {

    /**
     * Returns an iterator over all keys in this store. The keys may be
     * ordered but not necessarily.
     *
     * @return An iterator over all keys in this store.
     */
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException;

	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, KeySelector selector, StoreTransaction txh) throws StorageException;
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, StoreTransaction txh) throws StorageException;
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, StoreTransaction txh) throws StorageException;

    public void insert(ByteBuffer key, ByteBuffer value, StoreTransaction txh) throws StorageException;

    public void delete(ByteBuffer key, StoreTransaction txh) throws StorageException;

    public ByteBuffer get(ByteBuffer key, StoreTransaction txh) throws StorageException;

    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException;

    public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException;

    public ByteBuffer[] getLocalKeyPartition() throws StorageException;
    
    public String getName();

    public void close() throws StorageException;

}
