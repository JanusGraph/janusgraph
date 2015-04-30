package com.thinkaurelius.titan.graphdb.database.idassigner;

import java.time.Duration;
import java.util.concurrent.Callable;
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
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.IDBlock;

import com.thinkaurelius.titan.diskstorage.IDAuthority;

import com.thinkaurelius.titan.diskstorage.util.time.Temporals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIDPool implements IDPool {

    private static final Logger log =
            LoggerFactory.getLogger(StandardIDPool.class);


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
    private final int partition;
    private final int idNamespace;

    private final Duration renewTimeout;
    private final double renewBufferPercentage;

    private IDBlock currentBlock;
    private long currentIndex;
    private long renewBlockIndex;
//    private long nextID;
//    private long currentMaxID;
//    private long renewBufferID;

    private volatile IDBlock nextBlock;
    private Future<IDBlock> idBlockFuture;
    private final ThreadPoolExecutor exec;

    private volatile boolean closed;

    public StandardIDPool(IDAuthority idAuthority, int partition, int idNamespace, long idUpperBound, Duration renewTimeout, double renewBufferPercentage) {
        Preconditions.checkArgument(idUpperBound > 0);
        this.idAuthority = idAuthority;
        Preconditions.checkArgument(partition>=0);
        this.partition = partition;
        Preconditions.checkArgument(idNamespace>=0);
        this.idNamespace = idNamespace;
        this.idUpperBound = idUpperBound;
        Preconditions.checkArgument(!renewTimeout.isZero(), "Renew-timeout must be positive");
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
                        .setNameFormat("TitanID(" + partition + ")("+idNamespace+")[%d]")
                        .build());
        //exec.allowCoreThreadTimeOut(false);
        //exec.prestartCoreThread();
        idBlockFuture = null;

        closed = false;
    }

    private synchronized void waitForIDBlockGetter() throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        if (null != idBlockFuture) {
            try {
                nextBlock = idBlockFuture.get(renewTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) failed with an exception in %s",
                        partition, idNamespace, sw.stop());
                throw new TitanException(msg, e);
            } catch (TimeoutException e) {
                // Attempt to cancel the renewer
                idBlockFuture.cancel(true);
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) timed out in %s",
                        partition, idNamespace, sw.stop());
                throw new TitanException(msg, e);
            } catch (CancellationException e) {
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) was cancelled after %s",
                        partition, idNamespace, sw.stop());
                throw new TitanException(msg, e);
            } finally {
                idBlockFuture = null;
            }
            // Allow InterruptedException to propagate up the stack
        }
    }

    private synchronized void nextBlock() throws InterruptedException {
        assert currentIndex == currentBlock.numIds();
        Preconditions.checkState(!closed,"ID Pool has been closed for partition(%s)-namespace(%s) - cannot apply for new id block",
                partition,idNamespace);

        if (null == nextBlock && null == idBlockFuture) {
            startIDBlockGetter();
        }

        if (null == nextBlock) {
            waitForIDBlockGetter();
        }

        if (nextBlock == ID_POOL_EXHAUSTION)
            throw new IDPoolExhaustedException("Exhausted ID Pool for partition(" + partition+")-namespace("+idNamespace+")");

        currentBlock = nextBlock;
        currentIndex = 0;

        log.debug("ID partition({})-namespace({}) acquired block: [{}]", partition, idNamespace, currentBlock);

        assert currentBlock.numIds()>0;

        nextBlock = null;

        assert RENEW_ID_COUNT>0;
        renewBlockIndex = Math.max(0,currentBlock.numIds()-Math.max(RENEW_ID_COUNT, Math.round(currentBlock.numIds()*renewBufferPercentage)));
        assert renewBlockIndex<currentBlock.numIds() && renewBlockIndex>=currentIndex;
    }

    @Override
    public synchronized long nextID() {
        assert currentIndex <= currentBlock.numIds();

        if (currentIndex == currentBlock.numIds()) {
            try {
                nextBlock();
            } catch (InterruptedException e) {
                throw new TitanException("Could not renew id block due to interruption", e);
            }
        }

        if (currentIndex == renewBlockIndex) {
            startIDBlockGetter();
        }

        long returnId = currentBlock.getId(currentIndex);
        currentIndex++;
        if (returnId >= idUpperBound) throw new IDPoolExhaustedException("Reached id upper bound of " + idUpperBound);
        log.trace("partition({})-namespace({}) Returned id: {}", partition, idNamespace, returnId);
        return returnId;
    }

    @Override
    public synchronized void close() {
        closed=true;
        try {
            waitForIDBlockGetter();
        } catch (InterruptedException e) {
            throw new TitanException("Interrupted while waiting for id renewer thread to finish", e);
        }
        exec.shutdownNow();
    }

    private synchronized void startIDBlockGetter() {
        Preconditions.checkArgument(idBlockFuture == null, idBlockFuture);
        if (closed) return; //Don't renew anymore if closed
        //Renew buffer
        log.debug("Starting id block renewal thread upon {}", currentIndex);
        idBlockFuture = exec.submit(new IDBlockGetter(idAuthority, partition, idNamespace, renewTimeout));
    }

    private static class IDBlockGetter implements Callable<IDBlock> {

        private final Stopwatch alive;
        private final IDAuthority idAuthority;
        private final int partition;
        private final int idNamespace;
        private final Duration renewTimeout;

        public IDBlockGetter(IDAuthority idAuthority, int partition, int idNamespace, Duration renewTimeout) {
            this.idAuthority = idAuthority;
            this.partition = partition;
            this.idNamespace = idNamespace;
            this.renewTimeout = renewTimeout;
            this.alive = Stopwatch.createStarted();
        }

        @Override
        public IDBlock call() {
            Stopwatch running = Stopwatch.createStarted();

            try {
                IDBlock idBlock = idAuthority.getIDBlock(partition, idNamespace, renewTimeout);
                log.debug("Retrieved ID block from authority on partition({})-namespace({}), " +
                          "exec time {}, exec+q time {}",
                          partition, idNamespace, running.stop(), alive.stop());
                Preconditions.checkArgument(idBlock!=null && idBlock.numIds()>0);
                return idBlock;
            } catch (BackendException e) {
                throw new TitanException("Could not acquire new ID block from storage", e);
            } catch (IDPoolExhaustedException e) {
                return ID_POOL_EXHAUSTION;
            }
        }
    }
}
