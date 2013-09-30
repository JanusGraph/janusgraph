package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Buffers mutations against multiple {@link KeyColumnValueStore} from the same storage backend for increased
 * write performance. The buffer size (i.e. number of mutations after which to flush) is configurable.
 * <p/>
 * A BufferTransaction also attempts to flush multiple times in the event of temporary storage failures for increased
 * write robustness.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BufferTransaction implements StoreTransaction {


    private static final Logger log =
            LoggerFactory.getLogger(BufferTransaction.class);

    private final StoreTransaction tx;
    private final KeyColumnValueStoreManager manager;
    private final int bufferSize;
    private final int mutationAttempts;
    private final int attemptWaitTime;

    private int numMutations;
    private final Map<String, Map<StaticBuffer, KCVMutation>> mutations;

    public BufferTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager,
                             int bufferSize, int attempts, int waitTime) {
        this(tx, manager, bufferSize, attempts, waitTime, 8);
    }

    public BufferTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager,
                             int bufferSize, int attempts, int waitTime, int expectedNumStores) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(manager);
        Preconditions.checkArgument(bufferSize > 1, "Buffering only makes sense when bufferSize>1");
        this.tx = tx;
        this.manager = manager;
        this.numMutations = 0;
        this.bufferSize = bufferSize;
        this.mutationAttempts = attempts;
        this.attemptWaitTime = waitTime;
        this.mutations = new HashMap<String, Map<StaticBuffer, KCVMutation>>(expectedNumStores);
    }

    public StoreTransaction getWrappedTransactionHandle() {
        return tx;
    }

    public void mutate(String store, StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) throws StorageException {
        Preconditions.checkNotNull(store);
        if (additions.isEmpty() && deletions.isEmpty()) return;

        KCVMutation m = new KCVMutation(additions, deletions);
        Map<StaticBuffer, KCVMutation> storeMutation = mutations.get(store);
        if (storeMutation == null) {
            storeMutation = new HashMap<StaticBuffer, KCVMutation>();
            mutations.put(store, storeMutation);
        }
        KCVMutation existingM = storeMutation.get(key);
        if (existingM != null) {
            existingM.merge(m);
        } else {
            storeMutation.put(key, m);
        }

        numMutations += additions.size();
        numMutations += deletions.size();

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
            BackendOperation.execute(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    manager.mutateMany(mutations, tx);
                    return true;
                }

                @Override
                public String toString() {
                    return "BufferMutation";
                }
            }, mutationAttempts, attemptWaitTime);
            clear();
        }
    }

    private void clear() {
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            entry.getValue().clear();
        }
        numMutations = 0;
    }

    @Override
    public void commit() throws StorageException {
        flushInternal();
        tx.commit();
    }

    @Override
    public void rollback() throws StorageException {
        clear();
        tx.rollback();
    }

    @Override
    public StoreTxConfig getConfiguration() {
        return tx.getConfiguration();
    }
}
