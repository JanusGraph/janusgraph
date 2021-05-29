// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.util;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BackendOperation {

    private static final Logger log =
            LoggerFactory.getLogger(BackendOperation.class);
    private static final Random random = new Random();

    private static final Duration BASE_REATTEMPT_TIME= Duration.ofMillis(50);
    private static final double PERTURBATION_PERCENTAGE = 0.2;

    private static Duration pertubTime(Duration duration) {
        Duration newDuration = duration.dividedBy((int)(2.0 / (1 + (random.nextDouble() * 2 - 1.0) * PERTURBATION_PERCENTAGE)));
        assert !duration.isZero() : duration;
        return newDuration;
    }

    public static <V> V execute(Callable<V> exe, Duration totalWaitTime) throws JanusGraphException {
        try {
            return executeDirect(exe,totalWaitTime);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not execute operation due to backend exception",e);
        }
    }


    public static <V> V executeDirect(Callable<V> exe, Duration totalWaitTime) throws BackendException {
        Preconditions.checkArgument(!totalWaitTime.isZero(),"Need to specify a positive waitTime: %s",totalWaitTime);
        long maxTime = System.currentTimeMillis()+totalWaitTime.toMillis();
        Duration waitTime = pertubTime(BASE_REATTEMPT_TIME);
        BackendException lastException;
        while (true) {
            try {
                return exe.call();
            } catch (final Throwable e) {
                //Find inner-most StorageException
                Throwable ex = e;
                BackendException storeEx = null;
                do {
                    if (ex instanceof BackendException) storeEx = (BackendException)ex;
                } while ((ex=ex.getCause())!=null);
                if (storeEx!=null && storeEx instanceof TemporaryBackendException) {
                    lastException = storeEx;
                } else if (e instanceof BackendException) {
                    throw (BackendException)e;
                } else {
                    throw new PermanentBackendException("Permanent exception while executing backend operation "+exe.toString(),e);
                }
            }
            //Wait and retry
            assert lastException!=null;
            if (System.currentTimeMillis()+waitTime.toMillis()<maxTime) {
                log.info("Temporary exception during backend operation ["+exe.toString()+"]. Attempting backoff retry.",lastException);
                try {
                    Thread.sleep(waitTime.toMillis());
                } catch (InterruptedException r) {
                    // added thread interrupt signal to support traversal interruption
                    Thread.currentThread().interrupt();
                    throw new PermanentBackendException("Interrupted while waiting to retry failed backend operation", r);
                }
            } else {
                break;
            }
            waitTime = pertubTime(waitTime.multipliedBy(2));
        }
        throw new TemporaryBackendException("Could not successfully complete backend operation due to repeated temporary exceptions after "+totalWaitTime,lastException);
    }

    public static<R> R execute(Transactional<R> exe, TransactionalProvider provider, TimestampProvider times) throws BackendException {
        StoreTransaction txh = null;
        try {
            txh = provider.openTx();
            if (!txh.getConfiguration().hasCommitTime()) txh.getConfiguration().setCommitTime(times.getTime());
            return exe.call(txh);
        } catch (BackendException e) {
            if (txh!=null) txh.rollback();
            txh=null;
            throw e;
        } finally {
            if (txh!=null) txh.commit();
        }
    }

    public static<R> R execute(final Transactional<R> exe, final TransactionalProvider provider, final TimestampProvider times, Duration maxTime) throws JanusGraphException {
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


    public interface Transactional<R> {

        R call(StoreTransaction txh) throws BackendException;

    }

    public interface TransactionalProvider {

        StoreTransaction openTx() throws BackendException;

        void close() throws BackendException;

    }

}
