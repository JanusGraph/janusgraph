package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import org.junit.*;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class LogTest {

    private Logger log = LoggerFactory.getLogger(LogTest.class);

    public static final String DEFAULT_SENDER_ID = "sender";


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

    public void simpleSendReceive(int numMessages, int delayMS) throws Exception {
        Log log1 = manager.openLog("test1", ReadMarker.fromNow());
        assertEquals("test1",log1.getName());
        CountingReader count = new CountingReader();
        log1.registerReader(count);
        for (long i=1;i<=numMessages;i++) {
            log1.add(BufferUtil.getLongBuffer(i));
            System.out.println("Wrote message: " + i);
            Thread.sleep(delayMS);
        }
        Thread.sleep(11000);
        assertEquals(numMessages,count.totalMsg.get());
        assertEquals(numMessages*(numMessages+1)/2,count.totalValue.get());

        assertTrue(log1.unregisterReader(count));
        log1.close();

    }

    private static class CountingReader implements MessageReader {

        private AtomicLong totalMsg=new AtomicLong(0);
        private AtomicLong totalValue=new AtomicLong(0);

        @Override
        public void read(Message message) {
            assertNotNull(message);
            assertNotNull(message.getSenderId());
            assertTrue(System.currentTimeMillis()>=message.getTimestamp(TimeUnit.MILLISECONDS));
            StaticBuffer content = message.getContent();
            assertNotNull(content);
            assertEquals(8,content.length());
            long value = content.getLong(0);
            System.out.println("Read message: " + value);
            totalMsg.incrementAndGet();
            totalValue.addAndGet(value);
        }
    }


}
