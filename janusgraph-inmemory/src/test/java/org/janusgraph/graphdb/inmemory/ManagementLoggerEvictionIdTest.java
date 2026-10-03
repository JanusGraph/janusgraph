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

package org.janusgraph.graphdb.inmemory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.cache.SchemaCache;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.management.ManagementLogger;
import org.janusgraph.graphdb.database.management.MgmtLogType;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.types.system.BaseRelationType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

//An ordinary commit's cache eviction carries the eviction id 0. No instance acknowledges it, and the acknowledgement an
//instance of an earlier version sends for it is ignored, while an acknowledged eviction keeps its acknowledgement. The
//acknowledgements which wait for transactions to close share one thread, which closing the logger ends. The logger
//sends through a recording log and reads the messages it would receive back from it, so that what it sends can be
//checked without a second instance.
public class ManagementLoggerEvictionIdTest {

    private static final String OTHER_INSTANCE = "other-instance";

    //Records what the logger sends; a write may be held at a gate, as a slow one of a real log
    private static final class RecordingLog implements Log {
        private final List<StaticBuffer> sent = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch writeGate;
        private final CountDownLatch writeHeld = new CountDownLatch(1);

        @Override
        public Future<Message> add(StaticBuffer content) {
            final CountDownLatch gate = writeGate;
            if (gate != null) {
                writeHeld.countDown();
                try {
                    if (!gate.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("the write was never let through");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            sent.add(content);
            return CompletableFuture.completedFuture(message(OTHER_INSTANCE, content));
        }

        @Override
        public Future<Message> add(StaticBuffer content, StaticBuffer key) {
            return add(content);
        }

        @Override
        public void registerReader(ReadMarker readMarker, MessageReader... reader) {
        }

        @Override
        public void registerReaders(ReadMarker readMarker, Iterable<MessageReader> readers) {
        }

        @Override
        public boolean unregisterReader(MessageReader reader) {
            return false;
        }

        @Override
        public boolean unregisterReaderAndStopReadingProcess(MessageReader reader) {
            return false;
        }

        @Override
        public String getName() {
            return "recording";
        }

        @Override
        public void close() {
        }

        private List<StaticBuffer> awaitSent(int count, Duration timeout) throws InterruptedException {
            final long deadline = System.nanoTime() + timeout.toNanos();
            while (sent.size() < count && System.nanoTime() - deadline < 0) {
                Thread.sleep(50);
            }
            assertEquals(count, sent.size(), "messages sent within " + timeout);
            return sent;
        }
    }

    //Records the schema elements expired
    private static final class RecordingSchemaCache implements SchemaCache {
        private final List<Long> expired = new CopyOnWriteArrayList<>();

        @Override
        public Long getSchemaId(String schemaName) {
            return null;
        }

        @Override
        public EntryList getSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
            return EntryList.EMPTY_LIST;
        }

        @Override
        public void expireSchemaElement(long schemaId) {
            expired.add(schemaId);
        }
    }

    private static Message message(String senderId, StaticBuffer content) {
        final Instant timestamp = Instant.now();
        return new Message() {
            @Override
            public String getSenderId() {
                return senderId;
            }

            @Override
            public Instant getTimestamp() {
                return timestamp;
            }

            @Override
            public StaticBuffer getContent() {
                return content;
            }
        };
    }

    private StandardJanusGraph graph;
    private RecordingLog log;
    private RecordingSchemaCache schemaCache;
    private ManagementLogger logger;

    @BeforeEach
    public void setUp() {
        graph = (StandardJanusGraph) JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        log = new RecordingLog();
        schemaCache = new RecordingSchemaCache();
        logger = new ManagementLogger(graph, log, schemaCache, graph.getConfiguration().getTimestampProvider());
    }

    @AfterEach
    public void tearDown() {
        logger.close();
        graph.close();
    }

    @Test
    public void anUnacknowledgedEvictionIsProcessedButNotAcknowledged() throws Exception {
        logger.sendUnacknowledgedCacheEviction(Arrays.asList(11L, 12L));
        assertEquals(1, log.sent.size());

        logger.read(message(OTHER_INSTANCE, log.sent.get(0)));

        assertEquals(Arrays.asList(11L, 12L), schemaCache.expired);
        //An acknowledgement would be sent well within the second, as the acknowledged eviction below shows
        Thread.sleep(1000);
        assertEquals(1, log.sent.size(), "the unacknowledged eviction was acknowledged");
    }

    @Test
    public void anAcknowledgedEvictionIsAcknowledged() throws Exception {
        logger.sendCacheEviction(Collections.emptySet(), false, Collections.emptyList(),
            Collections.singleton(OTHER_INSTANCE));
        assertEquals(1, log.sent.size());

        logger.read(message(OTHER_INSTANCE, log.sent.get(0)));

        //No transaction is open, so the acknowledgement follows at once
        final ReadBuffer ack = log.awaitSent(2, Duration.ofSeconds(10)).get(1).asReadBuffer();
        final Serializer serializer = graph.getDataSerializer();
        assertEquals(MgmtLogType.CACHED_TYPE_EVICTION_ACK, serializer.readObjectNotNull(ack, MgmtLogType.class));
        assertEquals(OTHER_INSTANCE, serializer.readObjectNotNull(ack, String.class), "acknowledged to");
        assertEquals(1L, VariableLong.readPositive(ack), "acknowledged eviction id");
    }

    //The live threads, which the root thread group enumerates without their stack traces
    private static Set<Thread> liveThreads() {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        Thread[] threads = new Thread[root.activeCount() + 16];
        int count;
        while ((count = root.enumerate(threads, true)) == threads.length) {
            threads = new Thread[threads.length * 2];
        }
        return new HashSet<>(Arrays.asList(threads).subList(0, count));
    }

    //The acknowledging threads started since the given ones
    private static List<Thread> ackThreadsStartedSince(Set<Thread> before) {
        return liveThreads().stream()
            .filter(t -> !before.contains(t) && t.getName().startsWith("ManagementLogger-ack-"))
            .collect(Collectors.toList());
    }

    //The acknowledging threads started since the given ones, once one has: the executor starts its thread
    //asynchronously, so the poll is bounded rather than the wait fixed
    private static List<Thread> awaitAckThreadStartedSince(Set<Thread> before) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<Thread> started = ackThreadsStartedSince(before);
        while (started.isEmpty() && System.nanoTime() - deadline < 0) {
            Thread.sleep(10);
            started = ackThreadsStartedSince(before);
        }
        return started;
    }

    private void sendAcknowledgedEviction() {
        logger.sendCacheEviction(Collections.emptySet(), false, Collections.emptyList(),
            Collections.singleton(OTHER_INSTANCE));
    }

    @Test
    public void acknowledgementsWhichWaitForAnOpenTransactionShareOneThread() throws Exception {
        final int evictions = 10;
        final Set<Thread> before = liveThreads();
        final JanusGraphTransaction open = graph.newTransaction();
        try {
            for (int i = 0; i < evictions; i++) {
                sendAcknowledgedEviction();
            }
            for (int i = 0; i < evictions; i++) {
                logger.read(message(OTHER_INSTANCE, log.sent.get(i)));
            }

            //Each acknowledgement waits for the transaction, which was open when its eviction arrived: once the
            //acknowledging thread has started, nothing is acknowledged within a grace period
            final List<Thread> started = awaitAckThreadStartedSince(before);
            assertEquals(1, started.size(), "acknowledging threads started: " + started);
            assertTrue(started.get(0).isDaemon());
            Thread.sleep(500);
            assertEquals(evictions, log.sent.size(), "acknowledged while the transaction was open");
        } finally {
            open.rollback();
        }

        //Then all of them follow
        final List<StaticBuffer> sent = log.awaitSent(2 * evictions, Duration.ofSeconds(10));
        final Set<Long> acknowledged = new HashSet<>();
        final Serializer serializer = graph.getDataSerializer();
        for (StaticBuffer content : sent.subList(evictions, 2 * evictions)) {
            final ReadBuffer ack = content.asReadBuffer();
            assertEquals(MgmtLogType.CACHED_TYPE_EVICTION_ACK, serializer.readObjectNotNull(ack, MgmtLogType.class));
            assertEquals(OTHER_INSTANCE, serializer.readObjectNotNull(ack, String.class), "acknowledged to");
            acknowledged.add(VariableLong.readPositive(ack));
        }
        assertEquals(LongStream.rangeClosed(1, evictions).boxed().collect(Collectors.toSet()), acknowledged);
    }

    @Test
    public void closingTheLoggerDropsTheAcknowledgementsWhichWaitAndEndsItsThread() throws Exception {
        final Set<Thread> before = liveThreads();
        final JanusGraphTransaction open = graph.newTransaction();
        try {
            sendAcknowledgedEviction();
            logger.read(message(OTHER_INSTANCE, log.sent.get(0)));
            final List<Thread> started = awaitAckThreadStartedSince(before);
            assertEquals(1, started.size(), "acknowledging threads started: " + started);

            logger.close();

            started.get(0).join(10_000);
            assertFalse(started.get(0).isAlive(), "the acknowledging thread outlived the logger");
        } finally {
            open.rollback();
        }
        //An acknowledgement would be sent well within the second once the transaction closed, as the test above shows
        Thread.sleep(1000);
        assertEquals(1, log.sent.size(), "a dropped acknowledgement was sent");

        //Evictions which arrive later are applied, but not acknowledged
        sendAcknowledgedEviction();
        logger.sendUnacknowledgedCacheEviction(Collections.singletonList(21L));
        logger.read(message(OTHER_INSTANCE, log.sent.get(1)));
        logger.read(message(OTHER_INSTANCE, log.sent.get(2)));
        assertEquals(Collections.singletonList(21L), schemaCache.expired);
        Thread.sleep(1000);
        assertEquals(3, log.sent.size(), "an eviction which arrived after the logger closed was acknowledged");
    }

    //An acknowledgement which is being sent when the logger closes is a write of the log: close() waits for it, so
    //that the graph which closes next doesn't fail it
    @Test
    public void closingTheLoggerWaitsForAnAcknowledgementBeingSent() throws Exception {
        final JanusGraphTransaction open = graph.newTransaction();
        sendAcknowledgedEviction();
        logger.read(message(OTHER_INSTANCE, log.sent.get(0)));
        //The acknowledgement waits for the transaction; once that closes, its write is held at the gate
        final CountDownLatch writeMayFinish = new CountDownLatch(1);
        log.writeGate = writeMayFinish;
        open.rollback();
        assertTrue(log.writeHeld.await(10, TimeUnit.SECONDS), "the acknowledgement was not sent once the transaction closed");

        final CompletableFuture<Void> closed = CompletableFuture.runAsync(logger::close);
        Thread.sleep(300);
        assertFalse(closed.isDone(), "close() returned while an acknowledgement was being sent");

        writeMayFinish.countDown();
        closed.get(10, TimeUnit.SECONDS);
        assertEquals(2, log.sent.size(), "the acknowledgement being sent was lost");
    }

    //Waits until one of the errors contains the given text
    private static void awaitError(List<String> errors, String text) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (errors.stream().noneMatch(e -> e.contains(text)) && System.nanoTime() - deadline < 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
        assertTrue(errors.stream().anyMatch(e -> e.contains(text)), "errors: " + errors);
    }

    @Test
    public void anAcknowledgementGivesUpOnATransactionStillOpenAfterTheConfiguredTime() throws Exception {
        logger.close();
        logger = new ManagementLogger(graph, log, schemaCache, graph.getConfiguration().getTimestampProvider(),
            Duration.ofSeconds(120), true, Duration.ofMillis(200));
        final JanusGraphTransaction open = graph.newTransaction();
        try {
            captureErrors(errors -> {
                sendAcknowledgedEviction();
                logger.read(message(OTHER_INSTANCE, log.sent.get(0)));
                //Rather than after the default minute
                awaitError(errors, "waiting too long for transactions to close");
            });
        } finally {
            open.rollback();
        }
        //An acknowledgement would be sent well within the second once the transaction closed, as
        //acknowledgementsWhichWaitForAnOpenTransactionShareOneThread shows
        Thread.sleep(1000);
        assertEquals(1, log.sent.size(), "an acknowledgement which was given up on was sent");
    }

    @Test
    public void theAcknowledgementsOfAGraphGiveUpOnATransactionStillOpenAfterTheConfiguredTime() {
        final JanusGraph configured = JanusGraphFactory.build()
            .set("storage.backend", "inmemory")
            .set("graph.management-tx-close-wait-time", 200)
            //Schema changes arrive within a tenth of a second, rather than within the default five seconds
            .set("log.janusgraph.read-interval", 100)
            .set("log.janusgraph.send-delay", 0)
            .open();
        try {
            final JanusGraphManagement define = configured.openManagement();
            define.makePropertyKey("p").dataType(String.class).make();
            define.commit();
            final JanusGraphTransaction open = configured.newTransaction();
            try {
                captureErrors(errors -> {
                    //The instance acknowledges its own change too, once the open transaction has closed, which it
                    //doesn't before the wait ends
                    final JanusGraphManagement rename = configured.openManagement();
                    rename.changeName(rename.getPropertyKey("p"), "q");
                    rename.commit();
                    //Rather than after the default minute
                    awaitError(errors, "waiting too long for transactions to close");
                });
            } finally {
                open.rollback();
            }
        } finally {
            configured.close();
        }
    }

    @Test
    public void anAcknowledgementOfTheUnacknowledgedEvictionIsIgnored() {
        captureErrors(errors -> {
            //As an instance of an earlier version sends it: addressed to this instance, for eviction id 0
            logger.read(message(OTHER_INSTANCE, acknowledgement(0)));
            assertTrue(errors.isEmpty(), "the acknowledgement of eviction id 0 was not ignored: " + errors);

            //One for an eviction this instance never sent is still reported
            logger.read(message(OTHER_INSTANCE, acknowledgement(7)));
            assertEquals(1, errors.size(), "the acknowledgement of an unknown eviction was not reported");
            assertTrue(errors.get(0).contains("Could not find eviction trigger"), errors.get(0));
        });
    }

    //The acknowledgement of the given eviction id, addressed to this instance, as ManagementLogger sends it
    private StaticBuffer acknowledgement(long evictionId) {
        final DataOutput out = graph.getDataSerializer().getDataOutput(64);
        out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION_ACK);
        out.writeObjectNotNull(graph.getConfiguration().getUniqueGraphId());
        VariableLong.writePositive(out, evictionId);
        return out.getStaticBuffer();
    }

    //Runs the test with the errors ManagementLogger logs meanwhile collected; the tests route SLF4J to Log4j 2
    private static void captureErrors(Consumer<List<String>> test) {
        final String name = ManagementLogger.class.getName();
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = context.getConfiguration();
        final LoggerConfig existing = configuration.getLoggerConfig(name);
        final boolean ownConfig = name.equals(existing.getName());
        final LoggerConfig config = ownConfig ? existing : new LoggerConfig(name, Level.ERROR, true);
        final List<String> errors = new CopyOnWriteArrayList<>();
        final AbstractAppender appender = new AbstractAppender("errors", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                errors.add(event.getMessage().getFormattedMessage());
            }
        };
        appender.start();
        if (!ownConfig) configuration.addLogger(name, config);
        config.addAppender(appender, Level.ERROR, null);
        context.updateLoggers();
        try {
            test.accept(errors);
        } finally {
            config.removeAppender(appender.getName());
            appender.stop();
            if (!ownConfig) configuration.removeLogger(name);
            context.updateLoggers();
        }
    }
}
