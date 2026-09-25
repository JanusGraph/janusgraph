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
import org.janusgraph.core.JanusGraphFactory;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

//An ordinary commit's cache eviction carries the eviction id 0. No instance acknowledges it, and the acknowledgement an
//instance of an earlier version sends for it is ignored, while an acknowledged eviction keeps its acknowledgement. The
//logger sends through a recording log and reads the messages it would receive back from it, so that what it sends can
//be checked without a second instance.
public class ManagementLoggerEvictionIdTest {

    private static final String OTHER_INSTANCE = "other-instance";

    //Records what the logger sends
    private static final class RecordingLog implements Log {
        private final List<StaticBuffer> sent = new CopyOnWriteArrayList<>();

        @Override
        public Future<Message> add(StaticBuffer content) {
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
