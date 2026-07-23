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

package org.janusgraph.cdc;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.janusgraph.graphdb.database.index.CdcElementChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes CDC change records from Kafka and applies the implied mixed-index updates. One worker drives one Kafka
 * {@link Consumer}; run several workers in the same consumer group (across threads or processes) to scale out, with
 * Kafka partitioning the work.
 *
 * <p>Per poll batch it decodes records to {@link CdcElementChange}s, <strong>de-duplicates</strong> them, applies them
 * via {@link CdcIndexApplier} (with bounded exponential-backoff retry), and commits offsets only after the batch has
 * been processed successfully — including no-op batches that decode to no relevant changes (Kafka tombstones or
 * non-edgestore records), which are committed to make forward progress (<strong>at-least-once</strong>). If a batch
 * cannot be decoded or applied after the retry budget, the consumer is rewound to the batch start so the records are
 * reprocessed rather than skipped — guaranteeing the index eventually catches up rather than silently going stale.</p>
 */
public class CdcIndexUpdateWorker implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CdcIndexUpdateWorker.class);
    private static final AtomicInteger WORKER_COUNTER = new AtomicInteger();

    private final Consumer<byte[], byte[]> consumer;
    private final CdcEventDecoder decoder;
    private final CdcIndexApplier applier;
    private final CdcWorkerConfiguration config;
    private final int workerId = WORKER_COUNTER.incrementAndGet();

    private volatile boolean running = false;
    private volatile boolean shutdownInitiated = false;
    private volatile Thread thread;

    public CdcIndexUpdateWorker(Consumer<byte[], byte[]> consumer, CdcEventDecoder decoder,
                                CdcIndexApplier applier, CdcWorkerConfiguration config) {
        this.consumer = consumer;
        this.decoder = decoder;
        this.applier = applier;
        this.config = config;
    }

    /** Subscribes and starts the poll loop on a dedicated daemon thread. No-op after {@link #close()}. */
    public synchronized void start() {
        if (running || shutdownInitiated) {
            return;
        }
        running = true;
        thread = new Thread(this, "janusgraph-cdc-worker-" + workerId);
        thread.setDaemon(true);
        // run() only catches RuntimeExceptions; an Error (NoClassDefFoundError, OutOfMemoryError, ...) kills the
        // thread. Without this handler that death is invisible in the application log (daemon thread, JVM default
        // handler prints to stderr only) while the process keeps running and the index silently goes stale.
        thread.setUncaughtExceptionHandler((t, e) ->
            log.error("CDC worker thread {} died unexpectedly; this worker applies no further index updates until "
                + "the process is restarted (its partitions move to other consumers in the group, if any)", t.getName(), e));
        thread.start();
    }

    /** Whether the worker thread has been started and is still running. A worker that died from an unexpected
     *  {@link Error} (see the uncaught-exception handler in {@link #start()}) reports {@code false} here, letting
     *  embedders detect silent consumer loss. */
    public boolean isAlive() {
        final Thread t = thread;
        return t != null && t.isAlive();
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(config.getTopics());
            // A thread interrupt is treated as a shutdown signal (close() interrupts to cut a retry backoff short).
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    pollOnce();
                } catch (WakeupException e) {
                    if (running) {
                        log.warn("Unexpected consumer wakeup", e);
                    }
                    // otherwise a shutdown was requested via close()
                } catch (RuntimeException e) {
                    if (!running) {
                        // Shutdown in progress: the consumer may throw wakeup/interrupt-related errors; exit quietly.
                        break;
                    }
                    log.error("Error while processing a CDC batch; the batch will be reprocessed", e);
                    sleepQuietly(config.getRetryInitialWait().toMillis());
                }
            }
        } finally {
            // Clear a pending interrupt before closing: initiateShutdown() interrupts this thread (and sleepQuietly
            // re-asserts the flag), and KafkaConsumer.close() throws InterruptException when its caller is
            // interrupted -- aborting the graceful group leave (the group would then hold this member's partitions
            // until the session timeout) and escaping run() as an uncaught exception.
            final boolean interrupted = Thread.interrupted();
            try {
                consumer.close();
            } catch (RuntimeException e) {
                log.warn("Error closing the CDC Kafka consumer", e);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Executes a single poll-decode-apply-commit cycle. Package-private so the loop semantics can be unit-tested with a
     * {@code MockConsumer}.
     *
     * @return the number of de-duplicated element changes applied in this cycle
     */
    int pollOnce() {
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(config.getPollTimeout());
        if (records.isEmpty()) {
            return 0;
        }
        final Set<CdcElementChange> changes = new LinkedHashSet<>();
        try {
            for (ConsumerRecord<byte[], byte[]> record : records) {
                final Collection<CdcElementChange> decoded = decoder.decode(record.key(), record.value());
                if (decoded != null) {
                    changes.addAll(decoded);
                }
            }
            if (!changes.isEmpty()) {
                applyWithRetry(changes);
            }
        } catch (RuntimeException e) {
            // Decoding or applying the batch failed: rewind so the whole batch is reprocessed rather than skipped
            // (at-least-once); do not commit offsets. Note the consumer's position has already advanced past these
            // records, so without the rewind a later successful commitSync() would commit past them and lose them.
            // Malformed/undecodable records are handled inside the decoder (skipped) so they don't loop forever here.
            rewindToBatchStart(records);
            throw e;
        }
        consumer.commitSync();
        return changes.size();
    }

    /**
     * Applies the batch with a bounded exponential-backoff retry. This worker-level retry (plus the rewind in
     * {@link #pollOnce()}) is deliberately layered on top of the applier's own {@code BackendOperation} retry: the
     * inner one only retries temporary backend exceptions within a single apply attempt, while this one bounds the
     * total attempts and hands unrecoverable batches back to Kafka via the rewind.
     */
    private void applyWithRetry(Collection<CdcElementChange> changes) {
        int attempt = 0;
        while (true) {
            try {
                applier.apply(changes);
                return;
            } catch (RuntimeException e) {
                if (attempt >= config.getRetryLimit() || shutdownInitiated || Thread.currentThread().isInterrupted()) {
                    // Retry budget exhausted -- or a shutdown was requested, in which case retrying would only delay
                    // it: rethrow so pollOnce() rewinds (offsets uncommitted) and the batch is redelivered later.
                    throw e;
                }
                final long waitMs = backoffMillis(attempt);
                attempt++;
                log.warn("Applying {} CDC change(s) failed (attempt {}); retrying in {} ms",
                    changes.size(), attempt, waitMs, e);
                sleepQuietly(waitMs);
            }
        }
    }

    private long backoffMillis(int attempt) {
        final long initial = config.getRetryInitialWait().toMillis();
        final long max = config.getRetryMaxWait().toMillis();
        long wait = Math.max(initial, 1);
        for (int i = 0; i < attempt && wait < max; i++) {
            wait = Math.min(wait * 2, max);
        }
        return Math.min(wait, max);
    }

    private void rewindToBatchStart(ConsumerRecords<byte[], byte[]> records) {
        for (TopicPartition tp : records.partitions()) {
            final List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(tp);
            if (!partitionRecords.isEmpty()) {
                consumer.seek(tp, partitionRecords.get(0).offset());
            }
        }
    }

    /**
     * Signals the worker to stop without waiting for it: flips the running flag, wakes a blocking {@code poll()}, and
     * interrupts an in-flight retry backoff sleep. Follow with {@link #awaitTermination()}, or use {@link #close()}
     * which does both. Splitting the two lets several workers be signalled first and then awaited, so they shut down
     * concurrently instead of sequentially.
     *
     * <p>Synchronized (together with {@link #start()} and {@link #awaitTermination()}) so a close racing a start
     * cannot observe the half-initialized state between the {@code running} flip and the thread launch -- without
     * this, {@link #awaitTermination()} could take the never-started branch and close the consumer from this thread
     * while the freshly launched worker thread starts using it (KafkaConsumer permits only {@code wakeup()} from
     * another thread).</p>
     */
    public synchronized void initiateShutdown() {
        shutdownInitiated = true;
        running = false;
        try {
            consumer.wakeup();
        } catch (RuntimeException ignored) {
            // the consumer may already be shutting down
        }
        final Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
    }

    /** Waits (bounded) for the worker thread to exit. If the worker was never started, releases the consumer here.
     *  Synchronized for the same start/close race documented on {@link #initiateShutdown()}; the worker thread never
     *  takes this monitor, so holding it across the bounded join cannot deadlock. */
    public synchronized void awaitTermination() {
        final Thread t = thread;
        if (t == null) {
            // run()'s finally block is the normal owner of consumer.close(), but it never executes for a worker whose
            // thread was never started -- close the consumer directly so it cannot leak.
            try {
                consumer.close();
            } catch (RuntimeException e) {
                log.warn("Error closing the CDC Kafka consumer", e);
            }
            return;
        }
        try {
            t.join(config.getPollTimeout().toMillis() + 5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            log.warn("CDC worker thread {} did not stop within the shutdown timeout", t.getName());
        }
    }

    @Override
    public void close() {
        initiateShutdown();
        awaitTermination();
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
