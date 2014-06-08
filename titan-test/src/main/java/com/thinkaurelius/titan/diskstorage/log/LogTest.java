package com.thinkaurelius.titan.diskstorage.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import org.junit.*;
import org.junit.rules.TestName;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests general log implementations
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class LogTest {

    private static final Logger log = LoggerFactory.getLogger(LogTest.class);

    public static final String DEFAULT_SENDER_ID = "sender";

    private static final long TIMEOUT_MS = 30000;

    public abstract LogManager openLogManager(String senderId) throws StorageException;

    private LogManager manager;

    // This TestName field must be public.  Exception when I tried private:
    // "java.lang.Exception: The @Rule 'testName' must be public."
    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        log.debug("Starting {}.{}", getClass().getSimpleName(), testName.getMethodName());
        manager = openLogManager(DEFAULT_SENDER_ID);
    }

    @After
    public void shutdown() throws Exception {
        close();
        log.debug("Finished {}.{}", getClass().getSimpleName(), testName.getMethodName());
    }

    public void close() throws Exception {
        manager.close();
    }

    @Test
    public void smallSendReceive() throws Exception {
        simpleSendReceive(100,50);
    }

    @Test
    public void mediumSendReceive() throws Exception {
        simpleSendReceive(2000,1);
    }

    @Test
    public void testMultipleReadersOnSingleLog() throws Exception {
        sendReceive(4, 2000, 5);
    }

    @Test
    public void testReadMarkerResumesInMiddleOfLog() throws Exception {
        Log log1 = manager.openLog("test1", ReadMarker.fromNow());
        log1.add(BufferUtil.getLongBuffer(1L));
        log1.close();
        log1 = manager.openLog("test1", ReadMarker.fromNow());
        CountingReader count = new CountingReader(1, true);
        log1.registerReader(count);
        log1.add(BufferUtil.getLongBuffer(2L));
        count.await(TIMEOUT_MS);
        assertEquals(1, count.totalMsg.get());
        assertEquals(2, count.totalValue.get());
    }

    @Test
    public void testLogIsDurableAcrossReopen() throws Exception {
        final long past = System.currentTimeMillis() - 10L;
        final long future = past + 1000000L;
        Log l;
        l = manager.openLog("durable", ReadMarker.fromTime(future, TimeUnit.MILLISECONDS));
        l.add(BufferUtil.getLongBuffer(1L));
        manager.close();

        l = manager.openLog("durable", ReadMarker.fromTime(future, TimeUnit.MILLISECONDS));
        l.add(BufferUtil.getLongBuffer(2L));
        l.close();

        l = manager.openLog("durable", ReadMarker.fromTime(past, TimeUnit.MILLISECONDS));
        CountingReader count = new CountingReader(2, true);
        l.registerReader(count);
        count.await(TIMEOUT_MS);
        assertEquals(2, count.totalMsg.get());
        assertEquals(3L, count.totalValue.get());
    }

    @Test
    public void testMultipleLogsWithSingleReader() throws Exception {
        final int nl = 3;
        Log logs[] = new Log[nl];
        CountingReader count = new CountingReader(3, false);

        // Open all logs up front. This gets any ColumnFamily creation overhead
        // out of the way. This is particularly useful on HBase.
        for (int i = 0; i < nl; i++) {
            logs[i] = manager.openLog("ml" + i, ReadMarker.fromNow());
        }
        // Register readers
        for (int i = 0; i < nl; i++) {
            logs[i].registerReader(count);
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
        Log logs[] = new Log[n];
        CountingReader counts[] = new CountingReader[n];
        for (int i = 0; i < n; i++) {
            counts[i] = new CountingReader(1, true);
            logs[i] = manager.openLog("loner" + i, ReadMarker.fromNow());
        }
        for (int i = 0; i < n; i++) {
            logs[i].registerReader(counts[i]);
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
    public void testFuzzMessages() throws Exception {
        final int maxLen = 1024 * 4;
        final int rounds = 32;

        StoringReader reader = new StoringReader(rounds);
        List<StaticBuffer> expected = new ArrayList<StaticBuffer>(rounds);

        Log l = manager.openLog("fuzz", ReadMarker.fromNow());
        l.registerReader(reader);
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
            Thread.sleep(50L);
        }
        reader.await(TIMEOUT_MS);
        assertEquals(rounds, reader.msgCount);
        assertEquals(expected, reader.msgs);
    }

    @Test
    public void testUnregisterReader() throws Exception {
        Log log = manager.openLog("test1", ReadMarker.fromNow());

        // Register two readers and verify they receive messages.
        CountingReader reader1 = new CountingReader(1, true);
        CountingReader reader2 = new CountingReader(2, true);
        log.registerReader(reader1, reader2);
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

    private void simpleSendReceive(int numMessages, int delayMS) throws Exception {
        sendReceive(1, numMessages, delayMS);
    }

    public void sendReceive(int readers, int numMessages, int delayMS) throws Exception {
        Preconditions.checkState(0 < readers);
        Log log1 = manager.openLog("test1", ReadMarker.fromNow());
        assertEquals("test1",log1.getName());
        CountingReader counts[] = new CountingReader[readers];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = new CountingReader(numMessages, true);
            log1.registerReader(counts[i]);
        }
        for (long i=1;i<=numMessages;i++) {
            log1.add(BufferUtil.getLongBuffer(i));
//            System.out.println("Wrote message: " + i);
            Thread.sleep(delayMS);
        }
        for (int i = 0; i < counts.length; i++) {
            CountingReader count = counts[i];
            count.await(TIMEOUT_MS);
            assertEquals("counter index " + i + " message count mismatch", numMessages, count.totalMsg.get());
            assertEquals("counter index " + i + " value mismatch", numMessages*(numMessages+1)/2,count.totalValue.get());
            assertTrue(log1.unregisterReader(count));
        }
        log1.close();

    }

    /**
     * Test MessageReader implementation. Allows waiting until an expected number of messages have
     * been read.
     */
    private static class LatchMessageReader implements MessageReader {
        private final CountDownLatch latch;

        LatchMessageReader(int expectedMessageCount) {
            latch = new CountDownLatch(expectedMessageCount);
        }

        @Override
        public final void read(Message message) {
            assertNotNull(message);
            assertNotNull(message.getSenderId());
            assertNotNull(message.getContent());
            assertTrue(System.currentTimeMillis() >= message.getTimestamp(TimeUnit.MILLISECONDS));
            processMessage(message);
            latch.countDown();
        }

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

    private static class CountingReader extends LatchMessageReader {

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
            log.info("Read log value {} by senderid \"{}\"", value, message.getSenderId());
            if (expectIncreasingValues) {
                assertTrue("Message out of order or duplicated: " + lastMessageValue + " preceded " + value, lastMessageValue<value);
                lastMessageValue = value;
            }
            totalMsg.incrementAndGet();
            totalValue.addAndGet(value);
        }
    }

    private static class StoringReader extends LatchMessageReader {

        private List<StaticBuffer> msgs = new ArrayList<StaticBuffer>(64);
        private volatile int msgCount = 0;

        StoringReader(int expectedMessageCount) {
            super(expectedMessageCount);
        }

        @Override
        public void processMessage(Message message) {
            StaticBuffer content = message.getContent();
            msgs.add(content);
            msgCount++;
        }
    }
}
