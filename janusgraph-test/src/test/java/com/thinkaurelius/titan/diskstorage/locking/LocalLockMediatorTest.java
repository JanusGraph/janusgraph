package com.thinkaurelius.titan.diskstorage.locking;

import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProviders;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import org.junit.Test;

import java.time.Instant;

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
        TimestampProvider times = TimestampProviders.MICRO;
        LocalLockMediator<ExpectedValueCheckingTransaction> llm =
                new LocalLockMediator<ExpectedValueCheckingTransaction>(LOCK_NAMESPACE, times);

        assertTrue(llm.lock(kc, mockTx1, Instant.EPOCH));
        assertTrue(llm.lock(kc, mockTx2, Instant.MAX));

        llm = new LocalLockMediator<ExpectedValueCheckingTransaction>(LOCK_NAMESPACE, times);

        assertTrue(llm.lock(kc, mockTx1, Instant.MAX));
        assertFalse(llm.lock(kc, mockTx2, Instant.MAX));
    }
}
