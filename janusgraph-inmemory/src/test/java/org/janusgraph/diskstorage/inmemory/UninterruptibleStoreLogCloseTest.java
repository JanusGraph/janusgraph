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
import org.janusgraph.diskstorage.ResourceUnavailableException;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSManagerProxy;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSProxy;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

//BerkeleyJE invalidates its whole environment when a thread is interrupted in the middle of a storage operation, so a
//log on a store which does not support interruption must let its readers finish whatever operation they are in when
//it closes, however long that takes, rather than interrupt them after a second, and must not hold a monitor a reader
//may need meanwhile. The first two tests hold a read up for 3 s while the log closes, and the log warns about the
//wait meanwhile, as it should.
public class UninterruptibleStoreLogCloseTest {

    private static final String LOG_NAME = "testlog";

    //An in-memory store which claims not to support interruption, one of whose reads the test holds up
    private static class UninterruptibleStoreManager extends KCVSManagerProxy {
        private final Duration holdUp;
        private final CountDownLatch readHeldUp = new CountDownLatch(1);
        private final AtomicBoolean readInterrupted = new AtomicBoolean();
        private final AtomicBoolean heldReadFinished = new AtomicBoolean();
        private final AtomicBoolean holdUpNextRead = new AtomicBoolean();

        UninterruptibleStoreManager(Duration holdUp) {
            super(new InMemoryStoreManager());
            this.holdUp = holdUp;
        }

        @Override
        public StoreFeatures getFeatures() {
            return new StandardStoreFeatures.Builder(manager.getFeatures()).supportsInterruption(false).build();
        }

        @Override
        public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData)
                throws BackendException {
            return new KCVSProxy(manager.openDatabase(name, metaData)) {
                @Override
                public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
                    if (!holdUpNextRead.compareAndSet(true, false)) return super.getSlice(query, txh);
                    readHeldUp.countDown();
                    try {
                        Thread.sleep(holdUp.toMillis());
                    } catch (InterruptedException e) {
                        readInterrupted.set(true);
                        Thread.currentThread().interrupt();
                    }
                    final EntryList result = super.getSlice(query, txh);
                    heldReadFinished.set(true);
                    return result;
                }
            };
        }
    }

    private static final MessageReader IGNORING_READER = new MessageReader() {
        @Override
        public void read(Message message) {
        }

        @Override
        public void updateState() {
        }
    };

    private static KCVSLogManager openLogManager(UninterruptibleStoreManager storeManager) {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "closing");
        config.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, Duration.ofMillis(100), LOG_NAME);
        //No send thread, so that close() goes straight to the readers
        config.set(GraphDatabaseConfiguration.LOG_SEND_DELAY, Duration.ZERO, LOG_NAME);
        //A closing log warns every max-read-time, a second at the least, that it is still waiting for its readers
        config.set(KCVSLog.LOG_MAX_READ_TIME, Duration.ofSeconds(1), LOG_NAME);
        return new KCVSLogManager(storeManager, config.restrictTo(LOG_NAME));
    }

    private static Thread closerOf(Log log, AtomicReference<Throwable> closeFailure) {
        final Thread closer = new Thread(() -> {
            try {
                log.close();
            } catch (Throwable e) {
                closeFailure.set(e);
            }
        });
        closer.start();
        return closer;
    }

    @Test
    public void closeLetsAReaderFinishItsStorageOperation() throws Exception {
        //Longer than the second after which close() used to interrupt the reader, and than that second plus the
        //max-read-time set above, so that the wait warns more than once
        final UninterruptibleStoreManager storeManager = new UninterruptibleStoreManager(Duration.ofSeconds(3));
        final KCVSLogManager logManager = openLogManager(storeManager);
        try {
            final Log log = logManager.openLog(LOG_NAME);
            storeManager.holdUpNextRead.set(true);
            log.registerReader(ReadMarker.fromNow(), IGNORING_READER);
            assertTrue(storeManager.readHeldUp.await(10, TimeUnit.SECONDS));

            log.close();

            assertFalse(storeManager.readInterrupted.get(), "closing the log interrupted a read in progress");
            assertTrue(storeManager.heldReadFinished.get(), "closing the log returned before the read had finished");
        } finally {
            logManager.close();
            storeManager.close();
        }
    }

    @Test
    public void closeKeepsWaitingForAReaderWhenItIsInterrupted() throws Exception {
        final UninterruptibleStoreManager storeManager = new UninterruptibleStoreManager(Duration.ofSeconds(3));
        final KCVSLogManager logManager = openLogManager(storeManager);
        try {
            final Log log = logManager.openLog(LOG_NAME);
            storeManager.holdUpNextRead.set(true);
            log.registerReader(ReadMarker.fromNow(), IGNORING_READER);
            assertTrue(storeManager.readHeldUp.await(10, TimeUnit.SECONDS));

            final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            final Thread closer = closerOf(log, closeFailure);
            //Giving up on an interrupt would leave the caller to interrupt the reader instead
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (closer.isAlive() && System.nanoTime() - deadline < 0) {
                closer.interrupt();
                Thread.sleep(100);
            }
            closer.join(TimeUnit.SECONDS.toMillis(10));

            assertFalse(closer.isAlive(), "closing the log did not finish");
            assertNull(closeFailure.get());
            assertFalse(storeManager.readInterrupted.get(), "closing the log interrupted a read in progress");
            assertTrue(storeManager.heldReadFinished.get(), "closing the log returned before the read had finished");
        } finally {
            logManager.close();
            storeManager.close();
        }
    }

    @Test
    public void closeLetsAReaderOpenALogThroughTheManager() throws Exception {
        //The management log's readers open a management transaction, which opens the management log through its
        //manager, so a closing manager must not hold its monitor while one of its logs waits for the readers
        final UninterruptibleStoreManager storeManager = new UninterruptibleStoreManager(Duration.ZERO);
        final KCVSLogManager logManager = openLogManager(storeManager);
        Thread closer = null;
        try {
            final Log log = logManager.openLog(LOG_NAME);
            final CountDownLatch updating = new CountDownLatch(1);
            final CountDownLatch closing = new CountDownLatch(1);
            final AtomicBoolean openedLog = new AtomicBoolean();
            final AtomicReference<Throwable> updateFailure = new AtomicReference<>();
            log.registerReader(ReadMarker.fromNow(), new MessageReader() {
                @Override
                public void read(Message message) {
                }

                @Override
                public void updateState() {
                    if (updating.getCount() == 0) return;
                    updating.countDown();
                    try {
                        assertTrue(closing.await(10, TimeUnit.SECONDS));
                        logManager.openLog(LOG_NAME);
                        openedLog.set(true);
                    } catch (Throwable e) {
                        updateFailure.set(e);
                    }
                }
            });
            assertTrue(updating.await(10, TimeUnit.SECONDS));

            final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            closer = new Thread(() -> {
                try {
                    logManager.close();
                } catch (Throwable e) {
                    closeFailure.set(e);
                }
            });
            closer.start();
            //Let the closer reach its wait for the readers before the reader opens the log
            Thread.sleep(500);
            closing.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));

            assertFalse(closer.isAlive(), "closing the log manager waited for a reader which waited for its monitor");
            assertNull(closeFailure.get());
            assertNull(updateFailure.get());
            assertTrue(openedLog.get(), "the reader did not get to open the log");
        } finally {
            //Closing again would wait for a closer which never finished
            if (closer == null || !closer.isAlive()) logManager.close();
            storeManager.close();
        }
    }

    @Test
    public void closeLetsAReaderUnregisterItself() throws Exception {
        //A reader which unregisters itself, or registers another, takes the log's monitor, so a closing log must not
        //hold that while it waits for the readers
        final UninterruptibleStoreManager storeManager = new UninterruptibleStoreManager(Duration.ZERO);
        final KCVSLogManager logManager = openLogManager(storeManager);
        Thread closer = null;
        try {
            final Log log = logManager.openLog(LOG_NAME);
            final CountDownLatch reading = new CountDownLatch(1);
            final CountDownLatch closing = new CountDownLatch(1);
            final AtomicReference<Object> unregistered = new AtomicReference<>();
            final MessageReader reader = new MessageReader() {
                @Override
                public void read(Message message) {
                    reading.countDown();
                    try {
                        assertTrue(closing.await(10, TimeUnit.SECONDS));
                        unregistered.set(log.unregisterReader(this));
                    } catch (Throwable e) {
                        unregistered.set(e);
                    }
                }

                @Override
                public void updateState() {
                }
            };
            log.registerReader(ReadMarker.fromNow(), reader);
            log.add(BufferUtil.getLongBuffer(1));
            assertTrue(reading.await(10, TimeUnit.SECONDS));

            final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            closer = closerOf(log, closeFailure);
            //Let the closer reach its wait for the readers before the reader takes the monitor
            Thread.sleep(500);
            closing.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));

            assertFalse(closer.isAlive(), "closing the log waited for a reader which waited for its monitor");
            assertNull(closeFailure.get());
            //The reader found the log closed, or unregistered if it got in first
            final Object outcome = unregistered.get();
            assertTrue(outcome instanceof ResourceUnavailableException || Boolean.TRUE.equals(outcome),
                "the reader's unregistration ended in " + outcome);
        } finally {
            //Closing again would wait for a closer which never finished
            if (closer == null || !closer.isAlive()) logManager.close();
            storeManager.close();
        }
    }
}
