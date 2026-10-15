// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.server.util;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Creates a Gremlin pool for JanusGraph Server whose threads are virtual.
 * <p>
 * Gremlin Server evaluates requests in its Gremlin pool, a {@link ThreadPoolExecutor} of {@link Settings#gremlinPool}
 * platform threads with a queue of {@link Settings#maxWorkQueueSize} requests, which rejects a request when the queue
 * is full; Gremlin Server answers a rejected request with {@code TOO_MANY_REQUESTS}. The pool created here has the
 * same bounds and rejects requests the same way, and its threads are named like those of Gremlin Server's pool, but
 * they are virtual threads: a request which waits for the storage or index backend releases the platform thread it
 * ran on. {@code gremlinPool} can thus be raised to the number of requests which should be evaluated at once without
 * creating as many platform threads. Unlike the threads of Gremlin Server's pool, a thread ends after it has had no
 * request to run for a minute.
 */
public final class VirtualThreadGremlinPool {

    private static final Logger log = LoggerFactory.getLogger(VirtualThreadGremlinPool.class);

    /**
     * The prefix of the thread names of Gremlin Server's own Gremlin pool, which a counter starting at 1 follows.
     */
    public static final String THREAD_NAME_PREFIX = "gremlin-server-exec-";

    static final long KEEP_ALIVE_MILLIS = 60000;

    private VirtualThreadGremlinPool() {
    }

    /**
     * Creates the pool for the {@code gremlinPool} and {@code maxWorkQueueSize} of the given settings, where a
     * {@code gremlinPool} of 0 stands for the number of processors, as in Gremlin Server.
     *
     * @throws IllegalStateException if the running JVM is older than Java 24, see {@link VirtualThreads}
     * @throws IllegalArgumentException if {@code gremlinPool} is negative or {@code maxWorkQueueSize} isn't positive
     */
    public static ThreadPoolExecutor create(Settings settings) {
        if (!VirtualThreads.isSupported()) {
            throw new IllegalStateException("gremlinPoolVirtualThreads requires Java " + VirtualThreads.MIN_JAVA_VERSION
                + " or later, but JanusGraph Server runs on Java " + Runtime.version() + ". Before Java "
                + VirtualThreads.MIN_JAVA_VERSION + ", a virtual thread which waits inside a synchronized method keeps"
                + " its platform thread, as JanusGraph transactions do while they commit. Remove the setting or run"
                + " JanusGraph Server on Java " + VirtualThreads.MIN_JAVA_VERSION + " or later.");
        }
        int poolSize = settings.gremlinPool == 0 ? Runtime.getRuntime().availableProcessors() : settings.gremlinPool;
        ThreadPoolExecutor pool = create(poolSize, settings.maxWorkQueueSize,
            VirtualThreads.newThreadFactory(THREAD_NAME_PREFIX, 1));
        log.info("The Gremlin pool runs requests on virtual threads, up to {} at once with up to {} more waiting",
            poolSize, settings.maxWorkQueueSize);
        return pool;
    }

    static ThreadPoolExecutor create(int poolSize, int maxWorkQueueSize, ThreadFactory threadFactory) {
        Preconditions.checkArgument(poolSize > 0,
            "gremlinPool must be positive, or 0 for the number of processors: %s", poolSize);
        Preconditions.checkArgument(maxWorkQueueSize > 0, "maxWorkQueueSize must be positive: %s", maxWorkQueueSize);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(poolSize, poolSize, KEEP_ALIVE_MILLIS, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(maxWorkQueueSize), threadFactory, new ThreadPoolExecutor.AbortPolicy());
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }
}
