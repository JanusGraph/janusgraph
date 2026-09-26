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

package org.janusgraph.diskstorage.inmemory;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSManagerProxy;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSProxy;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

//Transaction recovery measures the time it gives a transaction by how far its log has been read, so the log must not
//report having got past a message which its readers are still processing, even while other read threads go on pulling,
//and must not stay behind a partition and bucket which is no longer read at all.
public class KCVSLogReadProgressTest {

    private static final String LOG_NAME = "testlog";

    @Test
    public void readProgressStaysBeforeAMessageTheReadersAreStillProcessing() throws Exception {
        final InMemoryStoreManager storeManager = new InMemoryStoreManager();
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "progress");
        config.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, Duration.ofMillis(100), LOG_NAME);
        config.set(KCVSLog.LOG_READ_LAG_TIME, Duration.ofMillis(50), LOG_NAME);
        config.set(GraphDatabaseConfiguration.LOG_SEND_DELAY, Duration.ZERO, LOG_NAME);
        //A second read thread goes on pulling while the first one is held up by the reader
        config.set(GraphDatabaseConfiguration.LOG_READ_THREADS, 2, LOG_NAME);
        final KCVSLogManager logManager = new KCVSLogManager(storeManager, config.restrictTo(LOG_NAME));
        try {
            final KCVSLog log = logManager.openLog(LOG_NAME);
            assertNull(log.getReadProgress());

            //The message with the first value is held up until released; the second read thread reads and processes
            //the one with the next value
            final long held = 42;
            final CountDownLatch received = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch receivedNext = new CountDownLatch(1);
            log.registerReader(ReadMarker.fromTime(Instant.now()), new MessageReader() {
                @Override
                public void read(Message message) {
                    if (message.getContent().getLong(0) != held) {
                        receivedNext.countDown();
                        return;
                    }
                    received.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public void updateState() {
                }
            });
            assertNotNull(log.getReadProgress());

            final Instant written = log.add(BufferUtil.getLongBuffer(held)).get(10, TimeUnit.SECONDS).getTimestamp();
            assertTrue(received.await(10, TimeUnit.SECONDS));

            //A later pull reads and processes the next message meanwhile, and still does not get the progress past
            //the one being processed
            final Instant writtenNext = log.add(BufferUtil.getLongBuffer(held + 1)).get(10, TimeUnit.SECONDS)
                .getTimestamp();
            assertTrue(receivedNext.await(10, TimeUnit.SECONDS));
            assertFalse(log.getReadProgress().isAfter(written), "the read progress passed a message in processing");

            release.countDown();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!log.getReadProgress().isAfter(writtenNext) && System.nanoTime() - deadline < 0) {
                Thread.sleep(50);
            }
            assertTrue(log.getReadProgress().isAfter(writtenNext),
                "the read progress did not move past the processed messages");
        } finally {
            logManager.close();
            storeManager.close();
        }
    }

    //An in-memory store whose message reads fail for good while the given flag is set; a store which fails a read
    //for good stops the pulls of the partition and bucket the read was for
    private static KCVSManagerProxy failingStoreManager(AtomicBoolean failReads, boolean onlyOnce) {
        return new KCVSManagerProxy(new InMemoryStoreManager()) {
            @Override
            public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData)
                    throws BackendException {
                return new KCVSProxy(manager.openDatabase(name, metaData)) {
                    @Override
                    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
                        if (onlyOnce ? failReads.compareAndSet(true, false) : failReads.get()) {
                            throw new PermanentBackendException("gone");
                        }
                        return super.getSlice(query, txh);
                    }
                };
            }
        };
    }

    //At least two partitions and buckets, each pulled on its own on the one read thread
    private static KCVSLogManager openLogManager(KCVSManagerProxy storeManager) {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "progress");
        config.set(GraphDatabaseConfiguration.LOG_NUM_BUCKETS, 2, LOG_NAME);
        config.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, Duration.ofMillis(100), LOG_NAME);
        config.set(KCVSLog.LOG_READ_LAG_TIME, Duration.ofMillis(50), LOG_NAME);
        config.set(GraphDatabaseConfiguration.LOG_SEND_DELAY, Duration.ZERO, LOG_NAME);
        return new KCVSLogManager(storeManager, config.restrictTo(LOG_NAME));
    }

    private static final MessageReader IGNORING_READER = new MessageReader() {
        @Override
        public void read(Message message) {
        }

        @Override
        public void updateState() {
        }
    };

    @Test
    public void readProgressLeavesOutAPartitionWhoseReadsFailedForGood() throws Exception {
        //The first message read fails for good, which stops the pulls of the partition and bucket it was for before
        //that one gets anywhere, and the progress must go on with the others instead of staying where it stopped
        final AtomicBoolean failNextRead = new AtomicBoolean();
        final KCVSManagerProxy storeManager = failingStoreManager(failNextRead, true);
        final KCVSLogManager logManager = openLogManager(storeManager);
        try {
            final KCVSLog log = logManager.openLog(LOG_NAME);
            //Armed before the pulls can start (opening the log has read its settings by now), so that the very first
            //read is the one which fails, and every partition and bucket starts here
            failNextRead.set(true);
            final Instant start = Instant.now();
            log.registerReader(ReadMarker.fromTime(start), IGNORING_READER);

            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!log.getReadProgress().isAfter(start) && System.nanoTime() - deadline < 0) {
                Thread.sleep(50);
            }
            assertFalse(failNextRead.get(), "no read failed");
            assertTrue(log.getReadProgress().isAfter(start), "the read progress stayed where the stopped one did");
        } finally {
            logManager.close();
            storeManager.close();
        }
    }

    @Test
    public void readProgressRunsOnWithTheClockOnceNothingIsReadAnyMore() throws Exception {
        //Every message read fails for good, from before the pulls can start, so no pull ever gets any partition and
        //bucket past where it started: only once every one has stopped can the progress get past that, and it must,
        //running on with the clock rather than staying there for good
        final AtomicBoolean failReads = new AtomicBoolean();
        final KCVSManagerProxy storeManager = failingStoreManager(failReads, false);
        final KCVSLogManager logManager = openLogManager(storeManager);
        try {
            final KCVSLog log = logManager.openLog(LOG_NAME);
            failReads.set(true);
            final Instant start = Instant.now();
            log.registerReader(ReadMarker.fromTime(start), IGNORING_READER);

            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!log.getReadProgress().isAfter(start) && System.nanoTime() - deadline < 0) {
                Thread.sleep(50);
            }
            final Instant seen = log.getReadProgress();
            assertTrue(seen.isAfter(start), "the read progress stayed where reading stopped");
            Thread.sleep(300);
            assertTrue(Duration.between(seen, log.getReadProgress()).compareTo(Duration.ofMillis(200)) >= 0,
                "the read progress did not run on with the clock");
        } finally {
            logManager.close();
            storeManager.close();
        }
    }
}
