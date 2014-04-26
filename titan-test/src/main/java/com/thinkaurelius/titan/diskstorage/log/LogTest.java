package com.thinkaurelius.titan.diskstorage.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import org.junit.*;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests general log implementations
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class LogTest {

    private Logger log = LoggerFactory.getLogger(LogTest.class);

    public static final String DEFAULT_SENDER_ID = "sender";

    private static final long SLEEP_TIME_MS = 22000;

    public abstract LogManager openLogManager(String senderId) throws StorageException;

    private LogManager manager;

    @Before
    public void setup() throws Exception {
        manager = openLogManager(DEFAULT_SENDER_ID);
    }

    @After
    public void shutdown() throws Exception {
        close();
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
        simpleSendReceive(2000,5);
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
        CountingReader count = new CountingReader();
        log1.registerReader(count);
        log1.add(BufferUtil.getLongBuffer(2L));
        Thread.sleep(SLEEP_TIME_MS);
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
        Thread.sleep(SLEEP_TIME_MS);
        manager.close();

        l = manager.openLog("durable", ReadMarker.fromTime(future, TimeUnit.MILLISECONDS));
        l.add(BufferUtil.getLongBuffer(2L));
        Thread.sleep(SLEEP_TIME_MS);
        l.close();

        l = manager.openLog("durable", ReadMarker.fromTime(past, TimeUnit.MILLISECONDS));
        CountingReader count = new CountingReader();
        l.registerReader(count);
        Thread.sleep(SLEEP_TIME_MS);
        assertEquals(2, count.totalMsg.get());
        assertEquals(3L, count.totalValue.get());
    }

    @Test
    public void testMultipleLogsWithSingleReader() throws Exception {
        final int nl = 3;
        Log logs[] = new Log[nl];
        long value = 1L;
        CountingReader count = new CountingReader(false);
        for (int i = 0; i < nl; i++) {
            logs[i] = manager.openLog("ml" + i, ReadMarker.fromNow());
            logs[i].registerReader(count);
            logs[i].add(BufferUtil.getLongBuffer(value));
            value <<= 1;
        }
        Thread.sleep(SLEEP_TIME_MS);
        assertEquals(3, count.totalMsg.get());
        assertEquals(value - 1, count.totalValue.get());
    }

    @Test
    public void testSeparateReadersAndLogsInSharedManager() throws Exception {
        final int n = 5;
        Log logs[] = new Log[n];
        CountingReader counts[] = new CountingReader[n];
        for (int i = 0; i < n; i++) {
            counts[i] = new CountingReader();
            logs[i] = manager.openLog("loner" + i, ReadMarker.fromNow());
            logs[i].registerReader(counts[i]);
            logs[i].add(BufferUtil.getLongBuffer(1L << (i + 1)));
        }
        Thread.sleep(SLEEP_TIME_MS);
        for (int i = 0; i < n; i++) {
            assertEquals(1L << (i + 1), counts[i].totalValue.get());
            assertEquals(1, counts[i].totalMsg.get());
        }
    }

    @Test
    public void testFuzzMessages() throws Exception {
        final int maxLen = 1024 * 4;
        final int rounds = 32;

        StoringReader reader = new StoringReader();
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
        Thread.sleep(SLEEP_TIME_MS);
        assertEquals(rounds, reader.msgCount);
        assertEquals(expected, reader.msgs);
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
            counts[i] = new CountingReader();
            log1.registerReader(counts[i]);
        }
        for (long i=1;i<=numMessages;i++) {
            log1.add(BufferUtil.getLongBuffer(i));
//            System.out.println("Wrote message: " + i);
            Thread.sleep(delayMS);
        }
        Thread.sleep(SLEEP_TIME_MS);
        for (int i = 0; i < counts.length; i++) {
            CountingReader count = counts[i];
            assertEquals("counter index " + i + " message count mismatch", numMessages, count.totalMsg.get());
            assertEquals("counter index " + i + " value mismatch", numMessages*(numMessages+1)/2,count.totalValue.get());
            assertTrue(log1.unregisterReader(count));
        }
        log1.close();

    }

    private static class CountingReader implements MessageReader {

        private static final Logger log =
                LoggerFactory.getLogger(CountingReader.class);

        private final AtomicLong totalMsg=new AtomicLong(0);
        private final AtomicLong totalValue=new AtomicLong(0);
        private final boolean expectIncreasingValues;

        private long lastMessageValue = 0;

        private CountingReader(boolean expectIncreasingValues) {
            this.expectIncreasingValues = expectIncreasingValues;
        }

        private CountingReader() {
            this(true);
        }

        @Override
        public void read(Message message) {
            assertNotNull(message);
            assertNotNull(message.getSenderId());
            assertTrue(System.currentTimeMillis()>=message.getTimestamp(TimeUnit.MILLISECONDS));
            StaticBuffer content = message.getContent();
            assertNotNull(content);
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

    private static class StoringReader implements MessageReader {

        private List<StaticBuffer> msgs = new ArrayList<StaticBuffer>(64);
        private volatile int msgCount = 0;

        @Override
        public void read(Message message) {
            assertNotNull(message);
            assertNotNull(message.getSenderId());
            assertTrue(System.currentTimeMillis()>=message.getTimestamp(TimeUnit.MILLISECONDS));
            StaticBuffer content = message.getContent();
            assertNotNull(content);
            msgs.add(content);
            msgCount++;
        }
    }
}
