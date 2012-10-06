package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BufferTransactionHandle implements StoreTransactionHandle {

    private final StoreTransactionHandle tx;
    private final BufferMutationKeyColumnValueStore store;
    private final int bufferSize;
    private int numMutations;
    

    private final Map<String,Map<ByteBuffer, Mutation>> mutations;

    public BufferTransactionHandle(StoreTransactionHandle tx, BufferMutationKeyColumnValueStore store, int bufferSize) {
        this(tx,store,bufferSize,8);
    }
    
    public BufferTransactionHandle(StoreTransactionHandle tx, BufferMutationKeyColumnValueStore store, int bufferSize, int expectedNumStores) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(store);
        Preconditions.checkArgument(bufferSize>1,"Buffering only makes sense when bufferSize>1");
        this.tx=tx;
        this.store=store;
        this.numMutations = 0;
        this.bufferSize=bufferSize;
        this.mutations = new HashMap<String,Map<ByteBuffer,Mutation>>(expectedNumStores);
    }

    public StoreTransactionHandle getWrappedTransactionHandle() {
        return tx;
    }

    public void mutate(String store, ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        Preconditions.checkNotNull(store);
        if ((additions==null || additions.isEmpty()) && (deletions==null || deletions.isEmpty())) return;

        Mutation m = new Mutation(additions,deletions);
        Map<ByteBuffer, Mutation> storeMutation = mutations.get(store);
        if (storeMutation==null) {
            storeMutation = new HashMap<ByteBuffer, Mutation>();
            mutations.put(store,storeMutation);
        }
        Mutation existingM = storeMutation.get(key);
        if (existingM!=null) {
            existingM.merge(m);
        } else {
            storeMutation.put(key, m);
        }

        if (additions!=null) numMutations+= additions.size();
        if (deletions!=null) numMutations+= deletions.size();

        if (numMutations >= bufferSize) {
            flushInternal();
        }
    }

    @Override
    public void flush()  throws StorageException{
        flushInternal();
        tx.flush();
    }

    private void flushInternal() throws StorageException {
        if (numMutations>0) {
            store.mutateMany(mutations,tx);
            clear();
        }
    }
    
    private void clear() {
        for (Map.Entry<String,Map<ByteBuffer,Mutation>> entry : mutations.entrySet()) {
            entry.getValue().clear();
        }
        numMutations = 0;
    }

    @Override
    public void commit() throws StorageException {
        flushInternal();
        tx.flush();
    }

    @Override
    public void abort() throws StorageException {
        clear();
        tx.abort();
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return tx.getConsistencyLevel();
    }
}
