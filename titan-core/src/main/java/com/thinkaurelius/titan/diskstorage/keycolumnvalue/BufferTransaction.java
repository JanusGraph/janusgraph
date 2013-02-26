package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BufferTransaction implements StoreTransaction {


    private static final Logger log =
            LoggerFactory.getLogger(BufferTransaction.class);

    private final StoreTransaction tx;
    private final BufferMutationKeyColumnValueStore store;
    private final int bufferSize;
    private final int mutationAttempts;
    private final int attemptWaitTime;

    private int numMutations;
    private final Map<String, Map<ByteBuffer, KCVMutation>> mutations;

    public BufferTransaction(StoreTransaction tx, BufferMutationKeyColumnValueStore store,
                             int bufferSize, int attempts, int waitTime) {
        this(tx, store, bufferSize, attempts, waitTime, 8);
    }

    public BufferTransaction(StoreTransaction tx, BufferMutationKeyColumnValueStore store,
                             int bufferSize, int attempts, int waitTime, int expectedNumStores) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(store);
        Preconditions.checkArgument(bufferSize > 1, "Buffering only makes sense when bufferSize>1");
        this.tx = tx;
        this.store = store;
        this.numMutations = 0;
        this.bufferSize = bufferSize;
        this.mutationAttempts = attempts;
        this.attemptWaitTime = waitTime;
        this.mutations = new HashMap<String, Map<ByteBuffer, KCVMutation>>(expectedNumStores);
    }

    public StoreTransaction getWrappedTransactionHandle() {
        return tx;
    }

    public void mutate(String store, ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        Preconditions.checkNotNull(store);
        if ((additions == null || additions.isEmpty()) && (deletions == null || deletions.isEmpty())) return;

        KCVMutation m = new KCVMutation(additions, deletions);
        Map<ByteBuffer, KCVMutation> storeMutation = mutations.get(store);
        if (storeMutation == null) {
            storeMutation = new HashMap<ByteBuffer, KCVMutation>();
            mutations.put(store, storeMutation);
        }
        KCVMutation existingM = storeMutation.get(key);
        if (existingM != null) {
            existingM.merge(m);
        } else {
            storeMutation.put(key, m);
        }

        if (additions != null) numMutations += additions.size();
        if (deletions != null) numMutations += deletions.size();

        if (numMutations >= bufferSize) {
            flushInternal();
        }
    }

    @Override
    public void flush() throws StorageException {
        flushInternal();
        tx.flush();
    }

    private void flushInternal() throws StorageException {
        if (numMutations > 0) {
            for (int attempt = 0; attempt < mutationAttempts; attempt++) {
                try {
                    store.mutateMany(mutations, tx);
                    clear();
                    break;
                } catch (TemporaryStorageException e) {
                    if (attempt + 1 >= mutationAttempts) {
                        throw new PermanentStorageException("Persisting " + numMutations + " failed " + mutationAttempts + " times. Giving up", e);
                    } else {
                        log.debug("Batch mutation failed. Retrying in {} ms. {}", attemptWaitTime, e);
                        if (attemptWaitTime > 0)
                            TimeUtility.sleepUntil(System.currentTimeMillis() + attemptWaitTime, null);
                    }
                }
            }
        }
    }

    private void clear() {
        for (Map.Entry<String, Map<ByteBuffer, KCVMutation>> entry : mutations.entrySet()) {
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
    public void rollback() throws StorageException {
        clear();
        tx.rollback();
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return tx.getConsistencyLevel();
    }
}
