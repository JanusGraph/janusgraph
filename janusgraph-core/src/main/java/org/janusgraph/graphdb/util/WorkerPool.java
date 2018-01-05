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

package org.janusgraph.graphdb.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for implementing a thread pool that closes gracefully and provides back-pressure when submitting jobs.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class WorkerPool implements AutoCloseable {

    private static final long SHUTDOWN_WAIT_MS = 10000;

    private final ThreadPoolExecutor processor;

    public WorkerPool(int numThreads) {
        processor = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(128));
        processor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void submit(Runnable runnable) {
        processor.submit(runnable);
    }

    @Override
    public void close() throws Exception {
        processor.shutdown();
        processor.awaitTermination(SHUTDOWN_WAIT_MS,TimeUnit.MILLISECONDS);
        if (!processor.isTerminated()) {
            //log.error("Processor did not terminate in time");
            processor.shutdownNow();
        }

    }
}
