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

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class for implementing a thread pool that closes gracefully and provides back-pressure when submitting jobs.
 * The pool waits for room in its queue, and {@link #close()} for its tasks, as long as tasks keep ending; it fails
 * where a task threw, or where no task ended for its progress timeout. Once a task threw, the tasks which haven't
 * started are skipped; a task which catches its failures and counts them, rather than throw, leaves the others to run.
 * One thread submits the tasks and then closes the pool, as a try-with-resources block does: {@link #submit} and
 * {@link #close()} aren't meant to be called at once.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class WorkerPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    //How many failures of tasks the exception which the pool throws holds besides its cause; the rest are only counted
    static final int MAX_SUPPRESSED_FAILURES = 10;
    //How often the pool logs that it is still waiting
    private static final long WAIT_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final String name;
    private final Duration progressTimeout;
    private final ThreadPoolExecutor processor;
    private final AtomicLong submitted = new AtomicLong();
    //The tasks which ended, run or skipped, which is the progress that the pool watches while it waits
    private final AtomicLong ended = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicInteger failureCount = new AtomicInteger();
    //The first failures, each instance once, as tasks may throw an instance which others share, see failure()
    private final List<Throwable> failures = new ArrayList<>();
    //Whether the pool stopped its tasks, as they made no progress or its thread was interrupted
    private volatile boolean stopped = false;

    public WorkerPool(int numThreads) {
        this("worker pool", numThreads, GraphDatabaseConfiguration.COMPUTER_TASK_PROGRESS_TIMEOUT.getDefaultValue());
    }

    /**
     * @param name what the pool's tasks do, which its log messages and exceptions name
     * @param numThreads the number of threads which run the tasks
     * @param progressTimeout how long the pool waits without any task ending, before it stops them and fails; one
     *                        longer than nanoseconds can hold, about 292 years, waits indefinitely
     */
    public WorkerPool(String name, int numThreads, Duration progressTimeout) {
        Preconditions.checkArgument(progressTimeout != null && !progressTimeout.isNegative() &&
            !progressTimeout.isZero(), "Invalid progress timeout: %s", progressTimeout);
        this.name = name;
        this.progressTimeout = progressTimeout;
        //Daemons, so that a task which never ends doesn't hold the JVM once the pool gave up on it, named for the pool
        final AtomicInteger threads = new AtomicInteger();
        processor = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(128), task -> {
                final Thread thread = new Thread(task, "WorkerPool-" + threads.getAndIncrement() + " of the " + name);
                thread.setDaemon(true);
                return thread;
            });
        //submit() puts the tasks into the queue itself, which the threads take them from
        processor.prestartAllCoreThreads();
    }

    /**
     * Queues the task for a thread of the pool, waiting while the queue is full, as long as tasks keep ending, see
     * {@link #close()}. What the task throws is kept for {@link #close()} to throw, and once a task threw, the tasks
     * which haven't started are skipped, as the caller fails anyway.
     */
    public void submit(Runnable runnable) {
        Preconditions.checkState(!processor.isShutdown(), "The %s is closed", name);
        final Runnable task = () -> {
            try {
                if (failureCount.get() > 0) {
                    skipped.incrementAndGet();
                } else {
                    runnable.run();
                }
            } catch (Throwable e) {
                addFailure(e);
            } finally {
                ended.incrementAndGet();
            }
        };
        //Rather than run the task on the caller's thread, where a task which never ends would be out of reach
        awaitProgress("room in its queue", slice -> processor.getQueue().offer(task, slice, TimeUnit.NANOSECONDS));
        //Counted once queued, as a task which the pool stopped waiting to queue never was
        submitted.incrementAndGet();
    }

    private void addFailure(Throwable e) {
        //Counted and recorded under one lock, so that the failures are recorded in the order in which they are counted,
        //and the first, the cause of the pool's failure, is the one which made the pool skip the tasks after it
        synchronized (failures) {
            failureCount.incrementAndGet();
            if (failures.size() <= MAX_SUPPRESSED_FAILURES && failures.stream().noneMatch(failure -> failure == e)) {
                failures.add(e);
            }
        }
    }

    /**
     * Waits for every task submitted to end, however long they take as long as some keep ending, as the work of a task
     * which never ran would be missing from the caller's results without an error. Where no task ends for the pool's
     * progress timeout, here or in {@link #submit(Runnable)}, the pool interrupts the running tasks, drops those which
     * haven't started and fails. Where the waiting thread is interrupted, it does the same, and keeps the thread's
     * interrupt status. Where tasks threw, it throws a {@link JanusGraphException} of its own, caused by the first
     * failure, or by the {@link InterruptedException} where the waiting thread was interrupted, with up to
     * {@value #MAX_SUPPRESSED_FAILURES} other failures suppressed; it doesn't change the failures, which may be
     * instances that other code shares.
     */
    @Override
    public void close() {
        if (stopped) {
            //The failure which stopped the pool was thrown already
            return;
        }
        processor.shutdown();
        awaitProgress("its tasks to end", slice -> processor.awaitTermination(slice, TimeUnit.NANOSECONDS));
        if (failureCount.get() > 0) {
            throw failure(null, null);
        }
    }

    private interface Attempt {
        //Whether the awaited event happened within the slice of time, in nanoseconds
        boolean happenedWithin(long slice) throws InterruptedException;
    }

    private void awaitProgress(String awaited, Attempt attempt) {
        final long timeout = saturatedNanos(progressTimeout);
        final long slice = Math.max(TimeUnit.MILLISECONDS.toNanos(1),
            Math.min(TimeUnit.SECONDS.toNanos(1), timeout / 10));
        long lastEnded = ended.get();
        long lastProgress = System.nanoTime();
        long lastLog = lastProgress;
        try {
            while (!attempt.happenedWithin(slice)) {
                final long now = System.nanoTime();
                final long endedNow = ended.get();
                if (endedNow != lastEnded) {
                    lastEnded = endedNow;
                    lastProgress = now;
                } else if (now - lastProgress >= timeout) {
                    final int running = processor.getActiveCount();
                    throw failure(String.format("No task of the %s ended for %s (computer.task-progress-timeout) " +
                        "while it waited for %s: it interrupted the %d which ran and dropped the %d which hadn't " +
                        "started", name, progressTimeout, awaited, running, stop()), null);
                }
                if (now - lastLog >= WAIT_LOG_INTERVAL_NANOS) {
                    log.info("The {} waits for {}, with {} running and {} queued tasks", name, awaited,
                        processor.getActiveCount(), processor.getQueue().size());
                    lastLog = now;
                }
            }
        } catch (InterruptedException e) {
            final int dropped = stop();
            Thread.currentThread().interrupt();
            throw failure(String.format("The %s was interrupted while it waited for %s: it interrupted the tasks " +
                "which ran and dropped the %d which hadn't started", name, awaited, dropped), e);
        }
    }

    //The duration in nanoseconds, or the longest one where it is longer, which the pool waits for indefinitely
    private static long saturatedNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    //How many tasks failed so far, each of them recorded before a later one is counted
    int failureCount() {
        return failureCount.get();
    }

    //Interrupts the running tasks and drops the others, returning how many it dropped
    private int stop() {
        stopped = true;
        return processor.shutdownNow().size();
    }

    private JanusGraphException failure(String stop, Throwable cause) {
        final List<Throwable> first;
        synchronized (failures) {
            first = new ArrayList<>(failures);
        }
        //The first failure is the cause, unless the pool stopped as its thread was interrupted, and up to
        //MAX_SUPPRESSED_FAILURES others are suppressed
        final Throwable primary = cause != null ? cause : first.isEmpty() ? null : first.get(0);
        final List<Throwable> suppressed = new ArrayList<>();
        for (Throwable failure : first) {
            if (failure != primary && suppressed.size() < MAX_SUPPRESSED_FAILURES) {
                suppressed.add(failure);
            }
        }
        final int attached = suppressed.size() + (cause == null && primary != null ? 1 : 0);
        final int failed = failureCount.get();
        final StringBuilder message = new StringBuilder();
        if (stop != null) {
            message.append(stop).append(failed > 0 ? "; " : "");
        }
        if (failed > 0) {
            message.append(String.format("%d of the %d tasks of the %s failed", failed, submitted.get(), name));
            final long skippedTasks = skipped.get();
            if (skippedTasks > 0) {
                message.append(String.format(", and the %d which hadn't started %s skipped", skippedTasks,
                    skippedTasks == 1 ? "was" : "were"));
            }
            if (failed > attached) {
                message.append(String.format("; %d of them %s attached, each exception once", attached,
                    attached == 1 ? "is" : "are"));
            }
        }
        final JanusGraphException exception = new JanusGraphException(message.toString(), primary);
        suppressed.forEach(exception::addSuppressed);
        return exception;
    }
}
