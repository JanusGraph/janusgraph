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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.janusgraph.graphdb.database.index.CdcElementChange;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CdcIndexUpdateWorkerLoopTest {

    private static final String TOPIC = "cassandra.janusgraph.edgestore";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private MockConsumer<byte[], byte[]> consumer;
    private RecordingApplier applier;
    private CdcWorkerConfiguration config;

    /** Decodes a record whose value's first byte is the vertex id into a single VERTEX change. */
    private final CdcEventDecoder decoder = (key, value) ->
        Collections.singletonList(new CdcElementChange(ElementCategory.VERTEX, (long) value[0]));

    @BeforeEach
    public void setUp() {
        consumer = new MockConsumer<>("earliest");
        applier = new RecordingApplier();
        config = CdcWorkerConfiguration.builder()
            .bootstrapServers("dummy:9092")
            .topics(Arrays.asList(TOPIC))
            .retryLimit(2)
            .retryInitialWait(Duration.ofMillis(1))
            .retryMaxWait(Duration.ofMillis(2))
            .pollTimeout(Duration.ofMillis(10))
            .build();
        consumer.assign(Collections.singletonList(TP));
        consumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));
    }

    private void addRecord(long offset, byte vertexId) {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, new byte[]{vertexId}, new byte[]{vertexId}));
    }

    private long committedOffset() {
        OffsetAndMetadata om = consumer.committed(Collections.singleton(TP)).get(TP);
        return om == null ? -1 : om.offset();
    }

    @Test
    public void dedupesBatchAndCommitsOffsets() {
        addRecord(0, (byte) 1);
        addRecord(1, (byte) 1);
        addRecord(2, (byte) 2);

        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(consumer, decoder, applier, config);
        int applied = worker.pollOnce();

        assertEquals(2, applied, "duplicate (V,1) collapsed");
        assertEquals(1, applier.calls.size(), "one apply call for the batch");
        assertEquals(2, applier.calls.get(0).size());
        assertEquals(3L, committedOffset(), "offsets committed after successful apply");
    }

    @Test
    public void retriesTransientApplyFailureThenCommits() {
        applier.failTimes = 1;
        addRecord(0, (byte) 7);

        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(consumer, decoder, applier, config);
        worker.pollOnce();

        assertEquals(2, applier.callCount, "failed once, then succeeded");
        assertEquals(1L, committedOffset());
    }

    @Test
    public void doesNotCommitWhenApplyExhaustsRetries() {
        applier.failTimes = Integer.MAX_VALUE; // always fail
        addRecord(0, (byte) 9);

        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(consumer, decoder, applier, config);
        assertThrows(RuntimeException.class, worker::pollOnce);

        assertEquals(3, applier.callCount, "initial attempt + 2 retries");
        assertNull(consumer.committed(Collections.singleton(TP)).get(TP), "offsets not committed on failure");
        assertEquals(0L, consumer.position(TP),
            "consumer rewound to the batch start so the exhausted batch is redelivered, not skipped (at-least-once)");
    }

    @Test
    public void commitsOffsetsForBatchesThatDecodeToNoChanges() {
        // The forward-progress half of the at-least-once contract: records that decode to zero relevant changes
        // (Kafka tombstones, non-edgestore tables) must still advance the committed offset -- otherwise a stretch
        // of irrelevant records would pin consumer lag forever and every restart would re-read it.
        addRecord(0, (byte) 1);
        addRecord(1, (byte) 2);
        CdcEventDecoder irrelevant = (key, value) -> Collections.emptyList();
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(consumer, irrelevant, applier, config);

        assertEquals(0, worker.pollOnce());

        assertEquals(0, applier.callCount, "nothing to apply");
        assertEquals(2L, committedOffset(), "no-op batches still commit offsets (forward progress)");
    }

    @Test
    public void rewindsEveryPartitionOfAFailedBatch() {
        // The rewind must seek EACH partition of the failed batch to its own first offset; a regression that only
        // handles one partition (or mixes offsets across partitions) would commit past unapplied records on the
        // others -- exactly the silent data loss the rewind exists to prevent.
        TopicPartition tp1 = new TopicPartition(TOPIC, 1);
        consumer.assign(Arrays.asList(TP, tp1));
        Map<TopicPartition, Long> beginnings = new HashMap<>();
        beginnings.put(TP, 0L);
        beginnings.put(tp1, 5L); // differing start offsets: a shared rewind offset cannot satisfy both
        consumer.updateBeginningOffsets(beginnings);
        addRecord(0, (byte) 1);
        addRecord(1, (byte) 2);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 5, new byte[]{3}, new byte[]{3}));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 6, new byte[]{4}, new byte[]{4}));
        CdcEventDecoder throwingDecoder = (key, value) -> {
            throw new RuntimeException("undecodable record");
        };
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(consumer, throwingDecoder, applier, config);

        assertThrows(RuntimeException.class, worker::pollOnce);

        assertEquals(0L, consumer.position(TP), "partition 0 rewound to its own batch start");
        assertEquals(5L, consumer.position(tp1), "partition 1 rewound to its own batch start");
        assertNull(consumer.committed(Collections.singleton(TP)).get(TP), "no offsets committed");
        assertNull(consumer.committed(Collections.singleton(tp1)).get(tp1), "no offsets committed");
    }

    @Test
    public void rewindsAndDoesNotCommitWhenDecodeFails() {
        addRecord(0, (byte) 3);
        addRecord(1, (byte) 4);
        CdcEventDecoder throwingDecoder = (key, value) -> {
            throw new RuntimeException("undecodable record");
        };
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(consumer, throwingDecoder, applier, config);

        assertThrows(RuntimeException.class, worker::pollOnce);

        assertEquals(0, applier.callCount, "apply is never invoked when decoding fails");
        assertNull(consumer.committed(Collections.singleton(TP)).get(TP), "offsets not committed on decode failure");
        assertEquals(0L, consumer.position(TP),
            "consumer rewound to the batch start so the records are reprocessed, not skipped (at-least-once)");
    }

    @Test
    public void emptyPollAppliesNothing() {
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(consumer, decoder, applier, config);
        assertEquals(0, worker.pollOnce());
        assertEquals(0, applier.callCount);
    }

    @Test
    public void closeWithoutStartClosesConsumer() {
        MockConsumer<byte[], byte[]> unusedConsumer = new MockConsumer<>("earliest");
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(unusedConsumer, decoder, applier, config);
        worker.close(); // never started: run()'s finally never executes, so close() itself must release the consumer
        assertTrue(unusedConsumer.closed(), "consumer of a never-started worker closed on close()");
    }

    @Test
    public void runLoopRecoversAndReappliesAfterAFailedBatch() throws InterruptedException {
        // The resilience loop itself: after a batch exhausts its retry budget (rewind + thrown exception), run()
        // must log, pause, and CONTINUE polling so the rewound batch is redelivered and eventually applied -- this
        // is what makes at-least-once actually converge. A worker that died on the first failed batch would pass
        // every pollOnce()-level test.
        MockConsumer<byte[], byte[]> subConsumer = new MockConsumer<>("earliest");
        CountDownLatch pastSecondCycle = new CountDownLatch(1);
        subConsumer.schedulePollTask(() -> {
            subConsumer.rebalance(Collections.singletonList(TP));
            subConsumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));
            subConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[]{5}, new byte[]{5}));
        });
        // Poll task 2 models the broker redelivering from the rewound position: unlike a real consumer, MockConsumer
        // clears its record buffer once polled, so the record must be re-added (the worker's own rewind seek()
        // meanwhile ensures the position accepts offset 0 again). Task 3 runs in the NEXT poll, i.e. strictly after
        // cycle 2's commitSync on the same worker thread -- a deterministic "past commit" point at which the
        // committed offset can be read safely (same thread as the consumer, before close() makes the MockConsumer
        // reject further queries).
        AtomicLong committedAfterRecovery = new AtomicLong(Long.MIN_VALUE);
        subConsumer.schedulePollTask(() ->
            subConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[]{5}, new byte[]{5})));
        subConsumer.schedulePollTask(() -> {
            OffsetAndMetadata committed = subConsumer.committed(Collections.singleton(TP)).get(TP);
            committedAfterRecovery.set(committed == null ? -1L : committed.offset());
            pastSecondCycle.countDown();
        });
        applier.failTimes = 3; // cycle 1 exhausts initial + 2 retries; the redelivered batch succeeds in cycle 2
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(subConsumer, decoder, applier, config);
        worker.start();
        try {
            assertTrue(pastSecondCycle.await(30, TimeUnit.SECONDS),
                "worker survived the failed batch and kept polling");
        } finally {
            worker.close();
        }
        assertEquals(1L, committedAfterRecovery.get(),
            "the rewound batch was redelivered and committed on the second cycle");
        assertEquals(4, applier.callCount, "3 failed attempts in cycle 1, success on redelivery in cycle 2");
    }

    @Test
    public void workerDeathByErrorIsObservableAndReleasesConsumer() throws InterruptedException {
        // An Error (OutOfMemoryError, NoClassDefFoundError, ...) is deliberately NOT swallowed by the poll loop:
        // the thread dies (logged by the uncaught-exception handler), which embedders detect via isAlive() -- the
        // supervision contract CdcIndexUpdateWorkerMain relies on. run()'s finally must still release the consumer.
        MockConsumer<byte[], byte[]> subConsumer = new MockConsumer<>("earliest");
        subConsumer.schedulePollTask(() -> {
            subConsumer.rebalance(Collections.singletonList(TP));
            subConsumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));
            subConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[]{6}, new byte[]{6}));
        });
        CdcIndexApplier lethal = changes -> {
            throw new NoClassDefFoundError("simulated classpath hole");
        };
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(subConsumer, decoder, lethal, config);
        worker.start();
        long deadline = System.currentTimeMillis() + 30_000;
        while (worker.isAlive() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertFalse(worker.isAlive(), "an Error must kill the worker thread (observable via isAlive())");
        worker.close(); // joins the dead thread, making the consumer state safe to inspect
        // (No offsets were committed for the fatally-failed batch -- commitSync only runs after a successful apply --
        // but that cannot be queried here: the closed MockConsumer rejects committed(). closed() itself proves
        // run()'s finally executed, which strictly precedes any commit for this cycle.)
        assertTrue(subConsumer.closed(), "run()'s finally released the consumer even on Error death");
    }

    @Test
    public void startThenCloseStopsCleanly() throws InterruptedException {
        // A fresh, subscription-mode consumer (the shared one is assign-mode for the pollOnce tests).
        MockConsumer<byte[], byte[]> subConsumer = new MockConsumer<>("earliest");
        // The scheduled poll task runs on the worker thread inside its first poll(), so the latch firing proves the
        // worker really entered the poll loop -- deterministic, unlike a fixed sleep.
        CountDownLatch polled = new CountDownLatch(1);
        subConsumer.schedulePollTask(() -> {
            subConsumer.rebalance(Collections.singletonList(TP));
            polled.countDown();
        });
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(subConsumer, decoder, applier, config);
        assertFalse(worker.isAlive(), "not alive before start()");
        worker.start();
        assertTrue(polled.await(10, TimeUnit.SECONDS), "worker entered the poll loop");
        assertTrue(worker.isAlive(), "alive while the poll loop runs (embedders monitor this)");
        worker.close();    // sets running=false, wakes the consumer, and joins the worker thread
        assertTrue(subConsumer.closed(), "consumer closed on shutdown");
        assertFalse(worker.isAlive(), "no longer alive after close()");
    }

    private static final class RecordingApplier implements CdcIndexApplier {
        final List<Collection<CdcElementChange>> calls = new ArrayList<>();
        int failTimes = 0;
        int callCount = 0;

        @Override
        public void apply(Collection<CdcElementChange> changes) {
            callCount++;
            calls.add(new ArrayList<>(changes));
            if (callCount <= failTimes) {
                throw new RuntimeException("transient failure #" + callCount);
            }
        }
    }
}
