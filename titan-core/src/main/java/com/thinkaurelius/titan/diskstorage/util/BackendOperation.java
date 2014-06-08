package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BackendOperation {

    private static final Logger log =
            LoggerFactory.getLogger(BackendOperation.class);
    private static final Random random = new Random();

    private static final Duration BASE_REATTEMPT_TIME=new StandardDuration(50,TimeUnit.MILLISECONDS);
    private static final double PERTURBATION_PERCENTAGE = 0.2;


    private static final Duration pertubateTime(Duration duration) {
        Duration newDuration = duration.multiply(1 + (random.nextDouble() * 2 - 1.0) * PERTURBATION_PERCENTAGE);
        assert !duration.isZeroLength() : duration;
        return newDuration;
    }

    public static final<V> V execute(Callable<V> exe, Duration totalWaitTime) throws TitanException {
        try {
            return executeDirect(exe,totalWaitTime);
        } catch (StorageException e) {
            throw new TitanException("Could not execute operation due to backend",e);
        }
    }


    public static final<V> V executeDirect(Callable<V> exe, Duration totalWaitTime) throws StorageException {
        Preconditions.checkArgument(!totalWaitTime.isZeroLength(),"Need to specify a positive waitTime: %s",totalWaitTime);
        long maxTime = System.currentTimeMillis()+totalWaitTime.getLength(TimeUnit.MILLISECONDS);
        Duration waitTime = pertubateTime(BASE_REATTEMPT_TIME);
        StorageException lastException = null;
        while (true) {
            try {
                return exe.call();
            } catch (StorageException e) {
                if (e instanceof TemporaryStorageException) {
                    lastException = e;
                } else {
                    throw e;
                }
            } catch (Throwable e) {
                throw new PermanentStorageException("Unexpected exception while executing backend operation "+exe.toString(),e);
            }
            //Wait and retry
            Preconditions.checkNotNull(lastException);
            if (System.currentTimeMillis()+waitTime.getLength(TimeUnit.MILLISECONDS)<maxTime) {
                log.info("Temporary exception during backend operation ["+exe.toString()+"]. Attempting backoff retry.",lastException);
                try {
                    Thread.sleep(waitTime.getLength(TimeUnit.MILLISECONDS));
                } catch (InterruptedException r) {
                    throw new PermanentStorageException("Interrupted while waiting to retry failed backend operation", r);
                }
            } else {
                break;
            }
            waitTime = pertubateTime(waitTime.multiply(2.0));
        }
        throw new TemporaryStorageException("Could not successfully complete backend operation due to repeated temporary exceptions after "+totalWaitTime,lastException);
    }

//    private static final double WAITTIME_PERTURBATION_PERCENTAGE = 0.5;
//    private static final double WAITTIME_PERTURBATION_PERCENTAGE_HALF = WAITTIME_PERTURBATION_PERCENTAGE/2;
//
//    public static final<V> V execute(Callable<V> exe, int maxRetryAttempts, Duration waitBetweenRetries) throws TitanException {
//        long retryWaittime = waitBetweenRetries.getLength(TimeUnit.MILLISECONDS);
//        Preconditions.checkArgument(maxRetryAttempts>0,"Retry attempts must be positive");
//        Preconditions.checkArgument(retryWaittime>=0,"Retry wait time must be non-negative");
//        int retryAttempts = 0;
//        StorageException lastException = null;
//        do {
//            try {
//                return exe.call();
//            } catch (StorageException e) {
//                if (e instanceof TemporaryStorageException) {
//                    lastException = e;
//                    log.debug("Temporary exception during backend operation", e);
//                } else {
//                    throw new TitanException("Permanent exception during backend operation",e); //Its permanent
//                }
//            } catch (Throwable e) {
//                throw new TitanException("Unexpected exception during backend operation",e);
//            }
//            //Wait and retry
//            retryAttempts++;
//            Preconditions.checkNotNull(lastException);
//            if (retryAttempts<maxRetryAttempts) {
//                long waitTime = Math.round(retryWaittime+((Math.random()*WAITTIME_PERTURBATION_PERCENTAGE-WAITTIME_PERTURBATION_PERCENTAGE_HALF)*retryWaittime));
//                Preconditions.checkArgument(waitTime>=0,"Invalid wait time: %s",waitTime);
//                log.info("Temporary storage exception during backend operation [{}]. Attempting incremental retry",exe.toString(),lastException);
//                try {
//                    Thread.sleep(waitTime);
//                } catch (InterruptedException r) {
//                    throw new TitanException("Interrupted while waiting to retry failed backend operation", r);
//                }
//            }
//        } while (retryAttempts<maxRetryAttempts);
//        throw new TitanException("Could not successfully complete backend operation due to repeated temporary exceptions after "+maxRetryAttempts+" attempts",lastException);
//    }

    public static<R> R execute(Transactional<R> exe, TransactionalProvider provider, TimestampProvider times) throws StorageException {
        StoreTransaction txh = null;
        try {
            txh = provider.openTx();
            if (!txh.getConfiguration().hasCommitTime()) txh.getConfiguration().setCommitTime(times.getTime());
            return exe.call(txh);
        } catch (StorageException e) {
            if (txh!=null) txh.rollback();
            txh=null;
            throw e;
        } finally {
            if (txh!=null) txh.commit();
        }
    }

    public static<R> R execute(final Transactional<R> exe, final TransactionalProvider provider, final TimestampProvider times, Duration maxTime) throws TitanException {
        return execute(new Callable<R>() {
            @Override
            public R call() throws Exception {
                return execute(exe,provider,times);
            }
            @Override
            public String toString() {
                return exe.toString();
            }
        },maxTime);
    }


    public static interface Transactional<R> {

        public R call(StoreTransaction txh) throws StorageException;

    }

    public static interface TransactionalProvider {

        public StoreTransaction openTx() throws StorageException;

        public void close() throws StorageException;

    }

}
