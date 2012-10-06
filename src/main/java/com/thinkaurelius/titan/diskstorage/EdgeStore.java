package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class EdgeStore implements IDAuthority, KeyColumnValueStore {

    private final KeyColumnValueStore store;


    @Override
    public long[] getIDBlock(int partition) throws StorageException {
        return new long[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long peekNextID(int partition) throws StorageException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransactionHandle txh) throws StorageException {
        return store.containsKey(key,txh);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, int limit, StoreTransactionHandle txh) throws StorageException {
        return store.getSlice(key,columnStart,columnEnd,limit,txh);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, StoreTransactionHandle txh) throws StorageException {
        return store.getSlice(key,columnStart,columnEnd,txh);
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransactionHandle txh) throws StorageException {
        return store.get(key,column,txh);
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransactionHandle txh) throws StorageException {
        return store.containsKeyColumn(key,column,txh);
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransactionHandle txh) throws StorageException {
        store.mutate(key,additions,deletions,txh);
    }

    @Override
    public void mutateMany(Map<ByteBuffer, Mutation> mutations, StoreTransactionHandle txh) throws StorageException {
        store.mutateMany(mutations,txh);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransactionHandle txh) throws StorageException {
        store.acquireLock(key,column,expectedValue,txh);
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransactionHandle txh) throws StorageException {
        return store.getKeys(txh);
    }

    @Override
    public void close() throws StorageException {
        store.close();
    }
}
