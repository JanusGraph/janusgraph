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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
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
        MockConsumer<byte[], byte[]> unusedConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(unusedConsumer, decoder, applier, config);
        worker.close(); // never started: run()'s finally never executes, so close() itself must release the consumer
        assertTrue(unusedConsumer.closed(), "consumer of a never-started worker closed on close()");
    }

    @Test
    public void startThenCloseStopsCleanly() throws InterruptedException {
        // A fresh, subscription-mode consumer (the shared one is assign-mode for the pollOnce tests).
        MockConsumer<byte[], byte[]> subConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        // The scheduled poll task runs on the worker thread inside its first poll(), so the latch firing proves the
        // worker really entered the poll loop -- deterministic, unlike a fixed sleep.
        CountDownLatch polled = new CountDownLatch(1);
        subConsumer.schedulePollTask(() -> {
            subConsumer.rebalance(Collections.singletonList(TP));
            polled.countDown();
        });
        CdcIndexUpdateWorker worker = new CdcIndexUpdateWorker(subConsumer, decoder, applier, config);
        worker.start();
        assertTrue(polled.await(10, TimeUnit.SECONDS), "worker entered the poll loop");
        worker.close();    // sets running=false, wakes the consumer, and joins the worker thread
        assertTrue(subConsumer.closed(), "consumer closed on shutdown");
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
