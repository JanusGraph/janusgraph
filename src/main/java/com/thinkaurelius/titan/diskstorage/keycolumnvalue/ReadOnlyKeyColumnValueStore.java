package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ReadOnlyKeyColumnValueStore implements KeyColumnValueStore {
	
	protected final KeyColumnValueStore store;
	
	public ReadOnlyKeyColumnValueStore(KeyColumnValueStore store) {
		this.store=store;
	}

	@Override
	public void close() throws StorageException {
		store.close();
	}

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue,
			StoreTransactionHandle txh) throws StorageException {
		throw new UnsupportedOperationException("Cannot lock on a read-only store");
	}

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransactionHandle txh) throws StorageException {
        return store.getKeys(txh);
    }

    @Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
			StoreTransactionHandle txh) throws StorageException {
		return store.containsKeyColumn(key, column, txh);
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column,
			StoreTransactionHandle txh) throws StorageException {
		return store.get(key, column, txh);
	}


    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransactionHandle txh) throws StorageException {
		throw new UnsupportedOperationException("Cannot mutate a read-only store");
	}

    @Override
    public void mutateMany(Map<ByteBuffer, Mutation> mutations, StoreTransactionHandle txh) throws StorageException {
        throw new UnsupportedOperationException("Cannot mutate a read-only store");
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransactionHandle txh) throws StorageException {
        return store.containsKey(key, txh);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, int limit, StoreTransactionHandle txh) throws StorageException {
        return store.getSlice(key, columnStart, columnEnd, limit, txh);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, StoreTransactionHandle txh) throws StorageException {
        return store.getSlice(key, columnStart, columnEnd, txh);
    }

}
