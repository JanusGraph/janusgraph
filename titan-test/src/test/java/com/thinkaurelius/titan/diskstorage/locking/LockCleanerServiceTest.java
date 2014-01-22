package com.thinkaurelius.titan.diskstorage.locking;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LockCleanerService;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.StandardLockCleanerRunnable;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.StandardLockCleanerService;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

public class LockCleanerServiceTest {
    private IMocksControl ctrl;
    private IMocksControl relaxedCtrl;;
    private KeyColumnValueStore store;
    private StoreTransaction tx;
    private ExecutorService exec;
    private LockCleanerService svc;

    private final ConsistentKeyLockerSerializer codec = new ConsistentKeyLockerSerializer();
    private final KeyColumn kc = new KeyColumn(
            new StaticArrayBuffer(new byte[]{(byte) 1}),
            new StaticArrayBuffer(new byte[]{(byte) 2}));

    @Before
    public void setupMocks() {
        relaxedCtrl = EasyMock.createControl();
        tx = relaxedCtrl.createMock(StoreTransaction.class);

        ctrl = EasyMock.createStrictControl();
        store = ctrl.createMock(KeyColumnValueStore.class);
        exec = ctrl.createMock(ExecutorService.class);
    }

    @After
    public void verifyMocks() {
        ctrl.verify();
    }

    @Test
    public void testCleanCooldownBlocksRapidRequests() {
        final long cutoff = 1L;

        svc = new StandardLockCleanerService(store, codec, exec, 60L, TimeUnit.SECONDS);

        expect(exec.submit(eq(new StandardLockCleanerRunnable(store, kc, tx, codec, cutoff)))).andReturn(null);

        ctrl.replay();

        for (int i = 0; i < 500; i++) {
            svc.clean(kc, cutoff, tx);
        }
    }

    @Test
    public void testCleanCooldownElapses() throws InterruptedException {
        final long cutoff = 1L;
        final long wait = 500L;

        svc = new StandardLockCleanerService(store, codec, exec, wait, TimeUnit.MILLISECONDS);

        expect(exec.submit(eq(new StandardLockCleanerRunnable(store, kc, tx, codec, cutoff)))).andReturn(null);

        expect(exec.submit(eq(new StandardLockCleanerRunnable(store, kc, tx, codec, cutoff + 1)))).andReturn(null);

        ctrl.replay();

        for (int i = 0; i < 2; i++) {
            svc.clean(kc, cutoff, tx);
        }

        Thread.sleep(wait + 1); // TODO use TimestampProvider

        for (int i = 0; i < 2; i++) {
            svc.clean(kc, cutoff + 1, tx);
        }
    }
}
