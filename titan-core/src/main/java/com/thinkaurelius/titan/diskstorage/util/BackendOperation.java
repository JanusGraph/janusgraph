package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BackendOperation {

    private static final Logger log =
            LoggerFactory.getLogger(BackendOperation.class);

    private static final long BASE_REATTEMPT_TIME_MS=50;

    public static final<V> V execute(Callable<V> exe, long maxTimeMS) throws TitanException {
        long waitTime = BASE_REATTEMPT_TIME_MS;
        long maxTime = System.currentTimeMillis()+maxTimeMS;
        StorageException lastException = null;
        do {
            try {
                return exe.call();
            } catch (StorageException e) {
                if (e instanceof TemporaryStorageException) lastException = e;
                else throw new TitanException("Permanent exception during backend operation",e); //Its permanent
            } catch (Throwable e) {
                throw new TitanException("Unexpected exception during backend operation",e);
            }
            //Wait and retry
            Preconditions.checkNotNull(lastException);
            if (System.currentTimeMillis()+waitTime<maxTime) {
                log.info("Temporary storage exception during backend operation. Attempting backoff retry",exe.toString(),lastException);
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException r) {
                    throw new TitanException("Interrupted while waiting to retry failed storage operation", r);
                }
            }
            waitTime*=2; //Exponential backoff
        } while (System.currentTimeMillis()<maxTime);
        throw new TitanException("Could not successfully complete backend operation due to repeated temporary exceptions after "+maxTimeMS+" ms",lastException);
    }

    private static final double WAITTIME_PERTURBATION_PERCENTAGE = 0.5;
    private static final double WAITTIME_PERTURBATION_PERCENTAGE_HALF = WAITTIME_PERTURBATION_PERCENTAGE/2;

    public static final<V> V execute(Callable<V> exe, int maxRetryAttempts, long retryWaittime) throws TitanException {
        Preconditions.checkArgument(maxRetryAttempts>0,"Retry attempts must be positive");
        Preconditions.checkArgument(retryWaittime>=0,"Retry wait time must be non-negative");
        int retryAttempts = 0;
        StorageException lastException = null;
        do {
            try {
                return exe.call();
            } catch (StorageException e) {
                if (e instanceof TemporaryStorageException) lastException = e;
                else throw new TitanException("Permanent exception during backend operation",e); //Its permanent
            } catch (Throwable e) {
                throw new TitanException("Unexpected exception during backend operation",e);
            }
            //Wait and retry
            retryAttempts++;
            Preconditions.checkNotNull(lastException);
            if (retryAttempts<maxRetryAttempts) {
                long waitTime = Math.round(retryWaittime+((Math.random()*WAITTIME_PERTURBATION_PERCENTAGE-WAITTIME_PERTURBATION_PERCENTAGE_HALF)*retryWaittime));
                Preconditions.checkArgument(waitTime>=0,"Invalid wait time: %s",waitTime);
                log.info("Temporary storage exception during backend operation [{}]. Attempting incremental retry",exe.toString(),lastException);
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException r) {
                    throw new TitanException("Interrupted while waiting to retry failed backend operation", r);
                }
            }
        } while (retryAttempts<maxRetryAttempts);
        throw new TitanException("Could not successfully complete backend operation due to repeated temporary exceptions after "+maxRetryAttempts+" attempts",lastException);
    }

    public static<R> R execute(Transactional<R> exe, TransactionalProvider provider) throws StorageException {
        StoreTransaction txh = null;
        try {
            txh = provider.openTx();
            return exe.call(txh);
        } catch (StorageException e) {
            if (txh!=null) txh.rollback();
            txh=null;
            throw e;
        } finally {
            if (txh!=null) txh.commit();
        }
    }


    public static interface Transactional<R> {

        public R call(StoreTransaction txh) throws StorageException;

    }

    public static interface TransactionalProvider {

        public StoreTransaction openTx() throws StorageException;

    }

}
