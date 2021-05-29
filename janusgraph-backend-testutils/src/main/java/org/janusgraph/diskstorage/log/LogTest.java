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

package org.janusgraph.diskstorage.log;

import com.google.common.base.Preconditions;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests general log implementations
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class LogTest {

    private static final Logger log = LoggerFactory.getLogger(LogTest.class);

    public static final String DEFAULT_SENDER_ID = "sender";

    private static final long TIMEOUT_MS = 30000;

    /**
     *
     * @param senderId The unique id identifying the sending instance
     * @param requiresOrderPreserving whether it is required by the test case that write order is preserved when reading.
     * @return
     * @throws BackendException
     */
    public abstract LogManager openLogManager(String senderId, boolean requiresOrderPreserving) throws BackendException;

    private LogManager manager;

    private static final String requiresOrderPreserving = "requiresOrderPreserving";

    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        //Tests that assume that write order is preserved when reading from the log must suffix their test names with "serial"
        Set<String> tags = testInfo.getTags();
        boolean requiresOrderPreserving = tags.contains(LogTest.requiresOrderPreserving);
        log.debug("Starting {}.{} - Order preserving {}", getClass().getSimpleName(), testInfo.getTestMethod().toString(), requiresOrderPreserving);
        manager = openLogManager(DEFAULT_SENDER_ID,requiresOrderPreserving);
    }

    @AfterEach
    public void shutdown(TestInfo testInfo) throws Exception {
        close();
        log.debug("Finished {}.{}", getClass().getSimpleName(), testInfo.getTestMethod().toString());
    }

    public void close() throws Exception {
        manager.close();
    }

    @Test
    @Tag(LogTest.requiresOrderPreserving)
    public void smallSendReceiveSerial() throws Exception {
        simpleSendReceive(100, 50);
    }

    @Test
    @Tag(LogTest.requiresOrderPreserving)
    public void mediumSendReceiveSerial() throws Exception {
        simpleSendReceive(2000,1);
    }

    @RepeatedIfExceptionsTest(repeats = 4, minSuccess = 2)
    @Tag(LogTest.requiresOrderPreserving)
    public void testMultipleReadersOnSingleLogSerial() throws Exception {
        sendReceive(4, 2000, 5, true, TIMEOUT_MS);
    }

    @Test
    public void testMultipleReadersOnSingleLog() throws Exception {
        sendReceive(4, 2000, 5, false, TIMEOUT_MS);
    }

    @Test
    public void testReadMarkerResumesInMiddleOfLog() throws Exception {
        Log log1 = manager.openLog("test1");
        log1.add(BufferUtil.getLongBuffer(1L));
        log1.close();
        log1 = manager.openLog("test1");
        CountingReader count = new CountingReader(1, true);
        log1.registerReader(ReadMarker.fromNow(),count);
        log1.add(BufferUtil.getLongBuffer(2L));
        count.await(TIMEOUT_MS);
        assertEquals(1, count.totalMsg.get());
        assertEquals(2, count.totalValue.get());
    }

    @Test
    @Tag(LogTest.requiresOrderPreserving)
    public void testLogIsDurableAcrossReopenSerial() throws Exception {
        final long past = System.currentTimeMillis() - 10L;
        Log l;
        l = manager.openLog("durable");
        l.add(BufferUtil.getLongBuffer(1L));
        manager.close();

        l = manager.openLog("durable");
        l.add(BufferUtil.getLongBuffer(2L));
        l.close();

        l = manager.openLog("durable");
        CountingReader count = new CountingReader(2, true);
        l.registerReader(ReadMarker.fromTime(Instant.ofEpochMilli(past)),count);
        count.await(TIMEOUT_MS);
        assertEquals(2, count.totalMsg.get());
        assertEquals(3L, count.totalValue.get());
    }

    @Test
    @Tag(LogTest.requiresOrderPreserving)
    public void testMultipleLogsWithSingleReaderSerial() throws Exception {
        final int nl = 3;
        Log[] logs = new Log[nl];
        CountingReader count = new CountingReader(3, false);

        // Open all logs up front. This gets any ColumnFamily creation overhead
        // out of the way. This is particularly useful on HBase.
        for (int i = 0; i < nl; i++) {
            logs[i] = manager.openLog("ml" + i);
        }
        // Register readers
        for (int i = 0; i < nl; i++) {
            logs[i].registerReader(ReadMarker.fromNow(),count);
        }
        // Send messages
        long value = 1L;
        for (int i = 0; i < nl; i++) {
            logs[i].add(BufferUtil.getLongBuffer(value));
            value <<= 1;
        }
        // Await receipt
        count.await(TIMEOUT_MS);
        assertEquals(3, count.totalMsg.get());
        assertEquals(value - 1, count.totalValue.get());
    }

    @Test
    public void testSeparateReadersAndLogsInSharedManager() throws Exception {
        final int n = 5;
        Log[] logs = new Log[n];
        CountingReader[] counts = new CountingReader[n];
        for (int i = 0; i < n; i++) {
            counts[i] = new CountingReader(1, true);
            logs[i] = manager.openLog("loner" + i);
        }
        for (int i = 0; i < n; i++) {
            logs[i].registerReader(ReadMarker.fromNow(),counts[i]);
            logs[i].add(BufferUtil.getLongBuffer(1L << (i + 1)));
        }
        // Check message receipt.
        for (int i = 0; i < n; i++) {
            log.debug("Awaiting CountingReader[{}]", i);
            counts[i].await(TIMEOUT_MS);
            assertEquals(1L << (i + 1), counts[i].totalValue.get());
            assertEquals(1, counts[i].totalMsg.get());
        }
    }

    @Test
    @Tag(LogTest.requiresOrderPreserving)
    public void testFuzzMessagesSerial() throws Exception {
        final int maxLen = 1024 * 4;
        final int rounds = 32;

        StoringReader reader = new StoringReader(rounds);
        final List<StaticBuffer> expected = new ArrayList<>(rounds);

        Log l = manager.openLog("fuzz");
        l.registerReader(ReadMarker.fromNow(),reader);
        Random rand = new Random();
        for (int i = 0; i < rounds; i++) {
            //int len = rand.nextInt(maxLen + 1);
            int len = maxLen;
            if (0 == len)
                len = 1; // 0 would throw IllegalArgumentException
            byte[] raw = new byte[len];
            rand.nextBytes(raw);
            StaticBuffer sb = StaticArrayBuffer.of(raw);
            l.add(sb);
            expected.add(sb);
            Thread.sleep(100L);
        }
        reader.await(TIMEOUT_MS);
        assertEquals(rounds, reader.msgCount);
        assertEquals(expected, reader.messages);
    }

    @Test
    public void testReadMarkerCompatibility() throws Exception {
        Log l1 = manager.openLog("testx");
        l1.registerReader(ReadMarker.fromIdentifierOrNow("mark"),new StoringReader(0));
        l1.registerReader(ReadMarker.fromIdentifierOrTime("mark", Instant.now().minusMillis(100)),new StoringReader(1));
        try {
            l1.registerReader(ReadMarker.fromIdentifierOrNow("other"));
            fail();
        } catch (IllegalArgumentException ignored) {}
        try {
            l1.registerReader(ReadMarker.fromTime(Instant.now().minusMillis(100)));
            fail();
        } catch (IllegalArgumentException ignored) {}
        l1.registerReader(ReadMarker.fromNow(), new StoringReader(2));
    }

    @Test
    @Tag(LogTest.requiresOrderPreserving)
    public void testUnregisterReaderSerial() throws Exception {
        Log log = manager.openLog("test1");

        // Register two readers and verify they receive messages.
        CountingReader reader1 = new CountingReader(1, true);
        CountingReader reader2 = new CountingReader(2, true);
        log.registerReader(ReadMarker.fromNow(),reader1, reader2);
        log.add(BufferUtil.getLongBuffer(1L));
        reader1.await(TIMEOUT_MS);

        // Unregister one reader. It should no longer receive messages. The other reader should
        // continue to receive messages.
        log.unregisterReader(reader1);
        log.add(BufferUtil.getLongBuffer(2L));
        reader2.await(TIMEOUT_MS);
        assertEquals(1, reader1.totalMsg.get());
        assertEquals(1, reader1.totalValue.get());
        assertEquals(2, reader2.totalMsg.get());
        assertEquals(3, reader2.totalValue.get());
    }

    protected void simpleSendReceive(int numMessages, int delayMS) throws Exception {
        sendReceive(1, numMessages, delayMS, true, LogTest.TIMEOUT_MS);
    }

    public void sendReceive(int readers, int numMessages, int delayMS, boolean expectMessageOrder, long timeoutMS) throws Exception {
        Preconditions.checkState(0 < readers);
        Log log1 = manager.openLog("test1");
        assertEquals("test1",log1.getName());
        CountingReader[] counts = new CountingReader[readers];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = new CountingReader(numMessages, expectMessageOrder);
            log1.registerReader(ReadMarker.fromNow(),counts[i]);
        }
        for (long i=1;i<=numMessages;i++) {
            log1.add(BufferUtil.getLongBuffer(i));
//            System.out.println("Wrote message: " + i);
            Thread.sleep(delayMS);
        }
        for (int i = 0; i < counts.length; i++) {
            CountingReader count = counts[i];
            count.await(timeoutMS);
            assertEquals(numMessages, count.totalMsg.get(), "counter index " + i + " message count mismatch");
            assertEquals(numMessages*(numMessages+1)/2,count.totalValue.get(), "counter index " + i + " value mismatch");
            assertTrue(log1.unregisterReader(count));
        }
        log1.close();

    }

    /**
     * Test MessageReader implementation. Allows waiting until an expected number of messages have
     * been read.
     */
    protected static class LatchMessageReader implements MessageReader {
        private final CountDownLatch latch;

        LatchMessageReader(int expectedMessageCount) {
            latch = new CountDownLatch(expectedMessageCount);
        }

        @Override
        public final void read(Message message) {
            assertNotNull(message);
            assertNotNull(message.getSenderId());
            assertNotNull(message.getContent());
            Instant now = Instant.now();
            assertTrue(now.isAfter(message.getTimestamp()) || now.equals(message.getTimestamp()));
            processMessage(message);
            latch.countDown();
        }

        @Override
        public void updateState() {}

        /**
         * Subclasses can override this method to perform additional processing on the message.
         */
        protected void processMessage(Message message) {}

        /**
         * Blocks until the reader has read the expected number of messages.
         *
         * @param timeoutMillis the maximum time to wait, in milliseconds
         * @throws AssertionError if the specified timeout is exceeded
         */
        public void await(long timeoutMillis) throws InterruptedException {
            if (latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                return;
            }
            long c = latch.getCount();
            Preconditions.checkState(0 < c); // TODO remove this, it's not technically correct
            String msg = "Did not read expected number of messages before timeout was reached (latch count is " + c + ")";
            log.error(msg);
            throw new AssertionError(msg);
        }
    }

    protected static class CountingReader extends LatchMessageReader {

        private static final Logger log =
                LoggerFactory.getLogger(CountingReader.class);

        private final AtomicLong totalMsg=new AtomicLong(0);
        private final AtomicLong totalValue=new AtomicLong(0);
        private final boolean expectIncreasingValues;

        private long lastMessageValue = 0;

        private CountingReader(int expectedMessageCount, boolean expectIncreasingValues) {
            super(expectedMessageCount);
            this.expectIncreasingValues = expectIncreasingValues;
        }

        @Override
        public void processMessage(Message message) {
            StaticBuffer content = message.getContent();
            assertEquals(8,content.length());
            long value = content.getLong(0);
            log.debug("Read log value {} by senderid \"{}\"", value, message.getSenderId());
            if (expectIncreasingValues) {
                assertTrue(lastMessageValue < value, "Message out of order or duplicated: " + lastMessageValue + " preceded " + value);
                lastMessageValue = value;
            }
            totalMsg.incrementAndGet();
            totalValue.addAndGet(value);
        }
    }

    protected static class StoringReader extends LatchMessageReader {

        private final List<StaticBuffer> messages = new ArrayList<>(64);
        private volatile int msgCount = 0;

        StoringReader(int expectedMessageCount) {
            super(expectedMessageCount);
        }

        @Override
        public void processMessage(Message message) {
            StaticBuffer content = message.getContent();
            messages.add(content);
            msgCount++;
        }
    }
}
