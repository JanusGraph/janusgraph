package com.thinkaurelius.titan.diskstorage.locking;

import com.thinkaurelius.titan.core.time.SimpleDuration;
import com.thinkaurelius.titan.core.time.Timepoint;
import com.thinkaurelius.titan.core.time.TimestampProvider;
import com.thinkaurelius.titan.core.time.Timestamps;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class LocalLockMediatorTest {

    private static final String LOCK_NAMESPACE = "test";
    private static final StaticBuffer LOCK_ROW = StaticArrayBuffer.of(new byte[]{1});
    private static final StaticBuffer LOCK_COL = StaticArrayBuffer.of(new byte[]{1});
    private static final KeyColumn kc = new KeyColumn(LOCK_ROW, LOCK_COL);
    //	private static final long LOCK_EXPIRATION_TIME_MS = 1;
//	private static final long SLEEP_MS = LOCK_EXPIRATION_TIME_MS * 1000;
    private static final ExpectedValueCheckingTransaction mockTx1 = mock(ExpectedValueCheckingTransaction.class);
    private static final ExpectedValueCheckingTransaction mockTx2 = mock(ExpectedValueCheckingTransaction.class);

    @Test
    public void testLockExpiration() throws InterruptedException {
        LocalLockMediator<ExpectedValueCheckingTransaction> llm =
                new LocalLockMediator<ExpectedValueCheckingTransaction>(LOCK_NAMESPACE, Timestamps.MICRO);

        assertTrue(llm.lock(kc, mockTx1, new Timepoint(0, TimeUnit.NANOSECONDS)));
        assertTrue(llm.lock(kc, mockTx2, new Timepoint(Long.MAX_VALUE, TimeUnit.NANOSECONDS)));

        llm = new LocalLockMediator<ExpectedValueCheckingTransaction>(LOCK_NAMESPACE, Timestamps.MICRO);

        assertTrue(llm.lock(kc, mockTx1, new Timepoint(Long.MAX_VALUE, TimeUnit.NANOSECONDS)));
        assertFalse(llm.lock(kc, mockTx2, new Timepoint(Long.MAX_VALUE, TimeUnit.NANOSECONDS)));
    }
}
