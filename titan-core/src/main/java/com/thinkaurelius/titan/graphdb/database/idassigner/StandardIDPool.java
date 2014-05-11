package com.thinkaurelius.titan.graphdb.database.idassigner;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.IDBlock;
import com.thinkaurelius.titan.util.time.Duration;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.StorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIDPool implements IDPool {

    private static final Logger log =
            LoggerFactory.getLogger(StandardIDPool.class);

    private static final TimeUnit SCHEDULING_TIME_UNIT =
            TimeUnit.MILLISECONDS; // TODO

    private static final IDBlock ID_POOL_EXHAUSTION = new IDBlock() {
        @Override
        public long numIds() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getId(long index) {
            throw new UnsupportedOperationException();
        }
    };

    private static final IDBlock UNINITIALIZED_BLOCK = new IDBlock() {
        @Override
        public long numIds() {
            return 0;
        }

        @Override
        public long getId(long index) {
            throw new ArrayIndexOutOfBoundsException(0);
        }
    };

    private static final int RENEW_ID_COUNT = 100;

    private final IDAuthority idAuthority;
    private final long idUpperBound; //exclusive
    private final int partitionID;

    private final Duration renewTimeout;
    private final double renewBufferPercentage;

    private IDBlock currentBlock;
    private long currentIndex;
    private long renewBlockIndex;
//    private long nextID;
//    private long currentMaxID;
//    private long renewBufferID;

    private volatile IDBlock nextBlock;
    private Future<?> idBlockFuture;
    private final ThreadPoolExecutor exec;

    private boolean initialized;

    public StandardIDPool(IDAuthority idAuthority, long partitionID, long idUpperBound, Duration renewTimeout, double renewBufferPercentage) {
        Preconditions.checkArgument(idUpperBound > 0);
        this.idAuthority = idAuthority;
        Preconditions.checkArgument(partitionID<(1l<<32));
        this.partitionID = (int) partitionID;
        this.idUpperBound = idUpperBound;
        Preconditions.checkArgument(!renewTimeout.isZeroLength(), "Renew-timeout must be positive");
        this.renewTimeout = renewTimeout;
        Preconditions.checkArgument(renewBufferPercentage>0.0 && renewBufferPercentage<=1.0,"Renew-buffer percentage must be in (0.0,1.0]");
        this.renewBufferPercentage = renewBufferPercentage;

        currentBlock = UNINITIALIZED_BLOCK;
        currentIndex = 0;
        renewBlockIndex = 0;

        nextBlock = null;

        // daemon=true would probably be fine too
        exec = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("TitanID[" + partitionID + "][%d]")
                        .build());
        //exec.allowCoreThreadTimeOut(false);
        //exec.prestartCoreThread();
        idBlockFuture = null;

        initialized = false;
    }

    private void waitForIDRenewer() throws InterruptedException {

        Stopwatch sw = new Stopwatch().start();
        if (null != idBlockFuture) {
            try {
                idBlockFuture.get(renewTimeout.getLength(SCHEDULING_TIME_UNIT), SCHEDULING_TIME_UNIT);
            } catch (ExecutionException e) {
                String msg = String.format("ID block allocation on partition %d failed with an exception in %s",
                        partitionID, sw.stop());
                throw new TitanException(msg, e);
            } catch (TimeoutException e) {
                // Attempt to cancel the renewer
                idBlockFuture.cancel(true);
                String msg = String.format("ID block allocation on partition %d timed out in %s",
                        partitionID, sw.stop());
                throw new TitanException(msg, e);
            } catch (CancellationException e) {
                String msg = String.format("ID block allocation on partition %d was cancelled after %s",
                        partitionID, sw.stop());
                throw new TitanException(msg, e);
            } finally {
                idBlockFuture = null;
            }
            // Allow InterruptedException to propagate up the stack
        }
    }

    private synchronized void nextBlock() throws InterruptedException {
        assert currentIndex == currentBlock.numIds();

        waitForIDRenewer();
        if (nextBlock == ID_POOL_EXHAUSTION)
            throw new IDPoolExhaustedException("Exhausted ID Pool for partition: " + partitionID);

        Preconditions.checkArgument(nextBlock!=null);

        currentBlock = nextBlock;
        currentIndex = 0;

        log.debug("[ID Partition {}] acquired block: [{}]", partitionID, currentBlock);

        assert currentBlock.numIds()>0;

        nextBlock = null;

        assert RENEW_ID_COUNT>0;
        renewBlockIndex = Math.max(0,currentBlock.numIds()-Math.max(RENEW_ID_COUNT, Math.round(currentBlock.numIds()*renewBufferPercentage)));
        assert renewBlockIndex<currentBlock.numIds() && renewBlockIndex>=currentIndex;
    }

    private void renewBuffer() {
        Preconditions.checkArgument(nextBlock == null, nextBlock);
        try {
            Stopwatch sw = new Stopwatch().start();
            IDBlock idBlock = idAuthority.getIDBlock(partitionID, renewTimeout);
            log.debug("Retrieved ID block from authority on partition {} in {}", partitionID, sw.stop());
            Preconditions.checkArgument(idBlock!=null && idBlock.numIds()>0);
            nextBlock = idBlock;
        } catch (StorageException e) {
            throw new TitanException("Could not acquire new ID block from storage", e);
        } catch (IDPoolExhaustedException e) {
            nextBlock = ID_POOL_EXHAUSTION;
        }
    }

    @Override
    public synchronized long nextID() {
        assert currentIndex <= currentBlock.numIds();
        if (!initialized) {
            startNextIDAcquisition();
            initialized = true;
        }

        if (currentIndex == currentBlock.numIds()) {
            try {
                nextBlock();
            } catch (InterruptedException e) {
                throw new TitanException("Could not renew id block due to interruption", e);
            }
        }

        if (currentIndex == renewBlockIndex) {
            startNextIDAcquisition();
        }
        long returnId = currentBlock.getId(currentIndex);
        currentIndex++;
        if (returnId >= idUpperBound) throw new IDPoolExhaustedException("Reached id upper bound of " + idUpperBound);
        log.trace("[{}] Returned id: {}", partitionID, returnId);
        return returnId;
    }

    @Override
    public synchronized void close() {
        //Wait for renewer to finish -- call exec.shutdownNow() instead?
        try {
            waitForIDRenewer();
        } catch (InterruptedException e) {
            throw new TitanException("Interrupted while waiting for id renewer thread to finish", e);
        }
        exec.shutdownNow();
    }

    private void startNextIDAcquisition() {
        Preconditions.checkArgument(idBlockFuture == null, idBlockFuture);
        //Renew buffer
        log.debug("Starting id block renewal thread upon {}", currentIndex);
        idBlockFuture = exec.submit(new IDBlockRunnable());
    }

    private class IDBlockRunnable implements Runnable {

        private final Stopwatch alive = new Stopwatch().start();

        @Override
        public void run() {
            Stopwatch running = new Stopwatch().start();
            renewBuffer();
            log.debug("Renewed buffer: partition {}, exec time {}, exec+q time {}", partitionID, running.stop(), alive.stop());
        }

    }

}
