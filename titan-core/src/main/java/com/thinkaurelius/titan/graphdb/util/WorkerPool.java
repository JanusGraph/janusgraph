package com.thinkaurelius.titan.graphdb.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for implementing a thread pool that closes gracefully and provides back-pressure when submitting jobs.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class WorkerPool implements AutoCloseable {

    private final ThreadPoolExecutor processor;
    private final long shutdownWaitMS = 10000;


    public WorkerPool(int numThreads) {
        processor = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(128));
        processor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void submit(Runnable runnable) {
        processor.submit(runnable);
    }

    @Override
    public void close() throws Exception {
        processor.shutdown();
        processor.awaitTermination(shutdownWaitMS,TimeUnit.MILLISECONDS);
        if (!processor.isTerminated()) {
            //log.error("Processor did not terminate in time");
            processor.shutdownNow();
        }

    }
}
