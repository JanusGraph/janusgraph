package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchStoreMutator extends BufferStoreMutator {

    private static final Logger log =
            LoggerFactory.getLogger(BatchStoreMutator.class);


    private final ExecutorService executor;
    
    private final AtomicInteger runningWorkers;

	public BatchStoreMutator(TransactionHandle txh,
                             MultiWriteKeyColumnValueStore edgeStore,
                             MultiWriteKeyColumnValueStore propertyIndex,
                             int bufferSize, int numThreads) {
		super(txh, edgeStore, propertyIndex, bufferSize);
        executor = Executors.newFixedThreadPool(numThreads);
        runningWorkers = new AtomicInteger(0);
	}

    @Override
    public void acquireEdgeLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) {
        throw new UnsupportedOperationException("Locking is not supported during batch operations");
    }

    @Override
    public void acquireIndexLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) {
        throw new UnsupportedOperationException("Locking is not supported during batch operations");
    }

	@Override
	public void flush() {
        super.flush();
        int waitTimeMS = 1;
        while (runningWorkers.get()>0) {
            log.debug("Waiting for {} workers to finish persisting data ({})",runningWorkers.get(),waitTimeMS);
            waitTimeMS*=2;
            try {
                Thread.sleep(waitTimeMS);
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for workers to persist data.");
                break;
            }
        }
	}

    protected void flushInternal() {
        if (!edgeMutations.isEmpty()) {
            executor.execute(new MakePeristenceCalls(edgeStore, edgeMutations));
            edgeMutations = new HashMap<ByteBuffer, Mutation>();
        }

        if (!indexMutations.isEmpty()) {
            executor.execute(new MakePeristenceCalls(propertyIndex, indexMutations));
            indexMutations = new HashMap<ByteBuffer, Mutation>();
        }

        numMutations = 0;
    }

    @Override
    protected void finalize() {
        executor.shutdown();
    }

    private class MakePeristenceCalls implements Runnable {

        private final MultiWriteKeyColumnValueStore store;
        private final Map<ByteBuffer, Mutation> mutations;

        public MakePeristenceCalls(MultiWriteKeyColumnValueStore store, Map<ByteBuffer, Mutation> mutations) {
            this.store=store;
            this.mutations=mutations;
            runningWorkers.incrementAndGet();
            log.debug("Initialized new peristence worker");
        }

        @Override
        public void run() {
            log.debug("Starting persistence...");
            store.mutateMany(mutations,txh);
            runningWorkers.decrementAndGet();
            log.debug("... stopped persistence.");
        }
    }

}
